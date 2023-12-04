import json
from typing import(List, Optional, Union, Dict)

from langchain_core.tracers.schemas import Run
from langchain_core.tracers.base import BaseTracer
from langchain_core.outputs import (ChatGenerationChunk, GenerationChunk)

from .base import ObserveSender


class ObserveTracer(BaseTracer):
    """Send structured data to Observe based on Langchain callbacks
    
    set the environment:
    
    OBSERVE_CUSTOMERID=1234567890
    OBSERVE_HOST=observeinc.com
    OBSERVE_AUTHTOKEN=ds1ASDLKJSALDKSJA.ASLDKJSALDKJSLAKJDLKSJADSLAKJD
    
    create ONE TRACER for your program, and pass it to all your workers/tools:
    
    tracer=ObserveTracer()
    llm = ChatOpenAI(temperature=0.5, model_name="gpt-4", callbacks=[tracer])
    text = "What does Observe Inc do?"
    print(llm.predict(text))
    
    See observe.senders.base.ObserveSender for more specifics.
    
    See https://js.langchain.com/docs/modules/callbacks/ for more information
    on tracers/callbacks.
    
    By default, streaming new tokens are not logged (because it generates a
    lot of events); if you want to log these, pass in log_new_tokens=True
    """
    def __init__(self, host:Optional[str]=None, customerid:Optional[str]=None, authtoken:Optional[str]=None, path:Optional[str]=None, accept_no_config:bool=False, log_sends:bool=False, log_new_tokens:bool=False):
        self.log_new_tokens = log_new_tokens
        self.sender = ObserveSender(host=host, customerid=customerid, authtoken=authtoken, path=path, accept_no_config=accept_no_config, log_sends=log_sends)
        self.sender.enqueue('starting', {})

    def close(self) -> None:
        """Flush and close the observe sender, forcing out any pending buffered events.

        By default, the sender will flush buffered events every few seconds, and also
        in an atexit() handler when your program ends. If you want to close a sender
        sooner, and flush out pending messages, call close() on it.
        """
        self.sender.close()

    sender: ObserveSender = None
    log_new_tokens: bool = False
    # run_map is needed by BaseTracer
    run_map: Dict[str, Run] = {}

    def _on_run_create(self, run: Run) -> None:
        self.sender.enqueue('run_create', self._run_to_dict(run=run, include_children=False))
    
    def _on_run_update(self, run: Run) -> None:
        self.sender.enqueue('run_update', self._run_to_dict(run=run, include_children=False))

    def _persist_run(self, run: Run) -> None:
        self.sender.enqueue('run_persist', self._run_to_dict(run=run, include_children=False))
    
    def _on_llm_start(self, run: Run) -> None:
        self.sender.enqueue('llm_start', self._run_to_dict(run=run, include_children=False))
    
    def _on_llm_new_token(self, run: Run, token:str, chunk:Optional[Union[GenerationChunk, ChatGenerationChunk]]) -> None:
        if self.log_new_tokens:
            self.sender.enqueue('llm_new_token', self._run_to_dict(run=run, include_children=False))
    
    def _on_llm_end(self, run: Run) -> None:
        self.sender.enqueue('llm_end', self._run_to_dict(run=run, include_children=False))

    def _on_llm_error(self, run:Run) -> None:
        self.sender.enqueue('llm_error', self._run_to_dict(run=run, include_children=False))
    
    def _on_chain_start(self, run: Run) -> None:
        self.sender.enqueue('chain_start', self._run_to_dict(run=run, include_children=False))

    def _on_chain_end(self, run: Run) -> None:
        self.sender.enqueue('chain_end', self._run_to_dict(run=run, include_children=False))

    def _on_chain_error(self, run:Run) -> None:
        self.sender.enqueue('chain_error', self._run_to_dict(run=run, include_children=False))

    def _on_tool_start(self, run: Run) -> None:
        self.sender.enqueue('tool_start', self._run_to_dict(run=run, include_children=False))

    def _on_tool_end(self, run: Run) -> None:
        self.sender.enqueue('tool_end', self._run_to_dict(run=run, include_children=False))

    def _on_tool_error(self, run:Run) -> None:
        self.sender.enqueue('tool_error', self._run_to_dict(run=run, include_children=False))

    def _on_retriever_start(self, run: Run) -> None:
        self.sender.enqueue('retriever_start', self._run_to_dict(run=run, include_children=False))

    def _on_retriever_end(self, run: Run) -> None:
        self.sender.enqueue('retriever_end', self._run_to_dict(run=run, include_children=False))

    def _on_retriever_error(self, run:Run) -> None:
        self.sender.enqueue('retriever_error', self._run_to_dict(run=run, include_children=False))

    def _run_to_dict(self, run:Run, include_children:bool) -> dict:
        data = json.loads(run.json())
        if (not include_children) and ('child_runs' in data):
            del data['child_runs']
        return data

