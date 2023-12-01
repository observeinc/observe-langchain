import atexit
import json
import os
import requests
import socket
import threading
import time
import traceback
import uuid
from typing import(List, Optional)

# Some tunables that are typically Just Fine (tm) the way they are,
# but can be tweaked for special circumstances.

# send a keepalive if nothing has been sent after this long
KEEPALIVE_INTERVAL = max(1.0, min(float(os.getenv("OBSERVE_KEEPALIVE_INTERVAL", "30.0")), 300.0))
# batch for at least this long before sending, for efficiency
BATCH_SEND_DELAY = max(1.0, min(float(os.getenv("OBSERVE_BATCH_SEND_DELAY", "1.0")), KEEPALIVE_INTERVAL))
# how much to save in backlogs if the HTTP interface fails (note
# that this is approximate)
MAX_BACKLOG_SAVE = min(1024*1024, max(0, int(os.getenv("OBSERVE_MAX_BACKLOG_SAVE", 256*1024))))

# The ObserveTracer you want to create and attach to your operations.
class ObserveSender(object):
    """Send structured data to Observe. See https://www.observeinc.com/

    You call enqueue() to send particular dicts of data (which must be JSON
    marshal-able) and they will be forwarded to Observe with a small amount
    of buffering. This may be preferable to taking the logs and parsing them
    as you can be more deliberate about what you send.

    You need configuration either in the constructor, or in the environment:

    OBSERVE_CUSTOMERID=1234567890
    OBSERVE_HOST=observeinc.com
    OBSERVE_AUTHTOKEN=ds1ASDLKJSALDKSJA.ASLDKJSALDKJSLAKJDLKSJADSLAKJD

    If you're reading the config from the environment, AND you want to accept
    the possibility that the environment may not be configured, and simply
    silently ignore this case, then pass in accept_no_config=True.

    Host should be the simple site domain name, or a full URL (which must
    include your customer ID, collector domain, and the https protocol) if you
    want to specify a particular collector path.  If the defaults are fine but
    you want to add a subpath, you can pass that as the 'path' argument here.
    The path will be available in the EXTRA parameters in your dataset.

    Example OBSERVE_HOST or 'host' argument with protocol and prefix path:
    https://1234567890.collect.eu1.observeinc.com/v1/http/myprefix

    Example OBSERVE_HOST or 'host' argument with just the site:
    eu1.observeinc.com

    The tracer will periodically flush queued data to Observe, but to make
    sure the last data enqueued gets properly sent, you should call close() on
    the ObserveSender and wait for it to return before your process exits. The
    sender will attempt to intercept atexit() but certain kinds of failures will prevent that from running.

    The package will include metadata in the dict you send, unless those keys are
    already populated by you. The metadata includes:
    - hostname (from the environment)
    - pid (of the process)
    - user (from the environment)
    - uuid (a random UUID for the sender instance)
    You can add these to a metadata sub-key with a name of your choice with the
    option metadata_key. If you specify a metadata_key that is the empty string,
    or that is the same as a field you're already providing, the metadata will
    not be included.
    """
    def __init__(self, host:Optional[str]=None, customerid:Optional[str]=None, authtoken:Optional[str]=None, path:Optional[str]=None, metadata_key:Optional[str]=None, accept_no_config:bool=False, log_sends=False):
        self.host = host or os.getenv("OBSERVE_HOST")
        self.customerid = customerid or os.getenv("OBSERVE_CUSTOMERID")
        self.authtoken = authtoken or os.getenv("OBSERVE_AUTHTOKEN")
        self.metadata_key = metadata_key
        self.accept_no_config = accept_no_config
        self.log_sends = log_sends
        myurl = self._derive_url(path)
        if not myurl:
            if self.accept_no_config:
                return
            self._raise_config_missing()
        self.url: str = myurl
        self.metadata = self._gather_metadata()
        self.queue: List[dict] = []
        self.cond = threading.Condition()
        self.last_send = time.time()
        atexit.register(self.close)
        self.sending: Optional[threading.Thread] = threading.Thread(target=self._send_thread, daemon=True, name="observe not closed")
        self.sending.start()

    def enqueue(self, data: dict) -> None:
        """enqueue a dict of data (must be JSON marshal-able) to be sent to the
        configured Observe collector"""
        with self.cond:
            # did I close already?
            if not self.sending:
                # it could be I'm just OK with not being available
                if self.accept_no_config:
                    return
                raise Exception('attempt to enqueue observations when already closed')
            timestamp = str(int(time.time()*1e9))
            if self.metadata_key:
                self.queue.append(merge(data, {self.metadata_key: merge(self.metadata, {"timestamp":timestamp})}))
            elif self.metadata_key != '':
                self.queue.append(merge(data, self.metadata, {"timestamp":timestamp}))
            else:
                self.queue.append(data)
            self.cond.notify()

    def close(self) -> None:
        """Flush all pending data and wait for it to be posted. If the final
        post fails, the data are lost."""
        with self.cond:
            if self.sending:
                atexit.unregister(self.close)
                thr = self.sending
                self.sending = None
                self.cond.notify()
        if thr:
            thr.join()

    def _raise_config_missing(self):
        m = []
        if not self.host:
            m.append("OBSERVE_HOST")
        if not self.customerid:
            m.append("OBSERVE_CUSTOMERID")
        if not self.authtoken:
            m.append("OBSERVE_AUTHTOKEN")
        raise Exception("ObserveTracer is missing configuration: " + ', '.join(m))

    def _derive_url(self, path: Optional[str]) -> Optional[str]:
        if not self._config_complete():
            return None
        url_path = f'/v1/http/python-sender'
        if path:
            url_path = f'/v1/http/{path.lstrip("/")}'
        if '/' in self.host:
            return self.host.rstrip('/') + url_path
        return f'https://{self.customerid}.collect.{self.host}{url_path}'

    def _config_complete(self) -> bool:
        return self.host and self.customerid and self.authtoken

    def _gather_metadata(self) -> dict:
        hn = os.getenv("HOSTNAME")
        if not hn:
            hn = socket.gethostname()
        return {
            "hostname": hn,
            "user": os.getenv("USER"),
            "pid": os.getpid(),
            "uuid": str(uuid.uuid4()),
        }

    def _send_thread(self) -> None:
        with self.cond:
            while self._maybe_send_one():
                self.cond.wait(BATCH_SEND_DELAY)
    
    def _maybe_send_one(self) -> bool:
        now = time.time()
        time_since_last_send = now - self.last_send
        # maybe generate a keepalive
        if self._is_time_for_keepalive(time_since_last_send):
            # nanoseconds as string is preferred format
            self.queue.append({'keepalive':str(now*1000000000)})
        # maybe send a batch, if we're quitting, or it's time
        if self._is_time_to_post(time_since_last_send):
            if self._do_post():
                # the batch succeeded
                self.last_send = time.time()
            elif not self.sending:
                # we're quitting, and the batch failed
                return False
        # return "keep going" if I'm still running OR if there's still data to send
        return self.sending or self.queue

    def _is_time_for_keepalive(self, time_since_last_send: float) -> bool:
        return not self.queue and time_since_last_send > KEEPALIVE_INTERVAL

    def _is_time_to_post(self, time_since_last_send: float) -> bool:
        return self.queue and ((not self.sending) or (time_since_last_send > BATCH_SEND_DELAY))

    # Note that _do_post releases the lock while posting, and thus new data may
    # arrive in the queue.
    def _do_post(self) -> bool:
        sendq = self.queue
        self.queue = []
        try:
            self.cond.release()
            body = ('\n'.join([maybe_json(x) for x in sendq]) + '\n').encode('utf-8')
            l = len(body)
            if self.log_sends:
                print(f'ObserveSender sending count={len(sendq)} observations with size={l} bytes', flush=True)
            response = requests.post(self.url, data=body, headers={
                'Content-Type': 'application/x-ndjson',
                'Content-Length': str(l),
                'Authorization': f'Bearer {self.authtoken}',
                })
            response.raise_for_status()
            self.cond.acquire()
            return True
        except Exception as e:
            traceback.print_exc()
            time.sleep(0.25) # prevent runaway exceptions
            self.cond.acquire()
            # put the messages back, try again later
            if l > MAX_BACKLOG_SAVE:
                # if we've collected too much baggage, cut it down
                numToDiscard = len(sendq)//2+1
                print(f'WARNING: ObserveSender discarding={numToDiscard} observations because failed to send={l} bytes: exception={repr(str(e))}', flush=True)
                sendq = sendq[numToDiscard:]
            sendq.extend(self.queue)
            self.queue = sendq
            return False

def merge(*srcs:dict) -> dict:
    dst = {}
    for src in srcs:
        for k, v in src.items():
            if not k in dst:
                dst[k] = v
    return dst

warned_unsupported = set()

def maybe_json(obj: dict) -> str:
    try:
        return json.dumps(obj, ensure_ascii=False, sort_keys=True)
    except Exception as e:
        # put something in the payload so you can sort it out later
        global warned_unsupported
        emsg = str(e)
        if not emsg in warned_unsupported:
            warned_unsupported.add(emsg)
            print(f'WARNING: ObserveTracer sending _unsupported because {emsg}', flush=True)
        return json.dumps({'_unsupported':str(obj), 'error':str(e)}, ensure_ascii=False, sort_keys=True)
