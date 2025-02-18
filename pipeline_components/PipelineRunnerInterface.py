from abc import ABC, abstractmethod

class PipelineRunnerInterface(ABC):
    
    @abstractmethod
    def run(self, pipeline):
        pass