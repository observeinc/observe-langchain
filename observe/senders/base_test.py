import base as observe

import time
from typing import Optional
import uuid

def test_sender(key:Optional[str]):
    print(f'making sender for key={repr(key)}', flush=True)
    os = observe.ObserveSender(host='observe-eng.com', customerid='101', authtoken='ds1UwwMKvCHqm8heaXDs:yETIaffDORqMBold5EoaBzNAgKbe3MQA', path=f'langchain-test{key}', metadata_key=key, log_sends=True)

    print('enqueuing data', flush=True)
    os.enqueue({'test': 'test'})
    # this is deliberately an unsupported data kind
    os.enqueue({'unsupported': uuid.uuid4()})

    for i in range(20):
        os.enqueue({'number': i})
        time.sleep(0.5)

    print('closing connection', flush=True)
    os.close()

    print(f'key {repr(key)} complete', flush=True)

test_sender(None)
test_sender('metadata')
test_sender('')

print('done', flush=True)
