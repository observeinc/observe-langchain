import json
from typing import(List, Optional, Union)

from langchain_core.tracers.schemas import Run
from langchain_core.tracers.base import BaseTracer
from langchain_core.outputs import (ChatGenerationChunk, GenerationChunk)

from .base import ObserveSender


class ObserveTracer(BaseTracer):
    def __init__(self, host:Optional[str]=None, customerid:Optional[str]=None, authtoken:Optional[str]=None, path:Optional[str]=None, accept_no_config:bool=False, log_sends:bool=False):
        self.sender = ObserveSender(host=host, customerid=customerid, authtoken=authtoken, path=path, accept_no_config=accept_no_config, log_sends=log_sends)

    def _on_run_create(self, run: Run) -> None:
        self.sender.enqueue('run_create', self._run_to_dict(run=run, include_children=False))
    
    def _on_run_update(self, run: Run) -> None:
        self.sender.enqueue('run_update', self._run_to_dict(run=run, include_children=False))

    def _persist_run(self, run: Run) -> None:
        self.sender.enqueue('run_persist', self._run_to_dict(run=run, include_children=False))
    
    def _on_llm_start(self, run: Run) -> None:
        self.sender.enqueue('llm_start', self._run_to_dict(run=run, include_children=False))
    
    def _on_llm_new_token(self, run: Run, token:str, chunk:Optional[Union[GenerationChunk, ChatGenerationChunk]]) -> None:
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
        return data



