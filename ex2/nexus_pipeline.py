from abc import ABC, abstractmethod
from typing import Any, List, Dict, Union, Optional, Protocol


class ProcessingStage (Protocol):

    def process(self, data: Any) -> Any:
        ...


class ProcessingPipeline(ABC):
    def __init__(self):
        self.stages: List[ProcessingStage] = []

    def add_stage(self, stage: ProcessingStage):
        self.stages.append(stage)

    @abstractmethod
    def process(self, data) -> Any:
        pass


class InputStage():

    def __init__(self):
        print("Stage 1: Input validation and parsing")

    def process(self, data: Any) -> Any:
        try:
            data["valid"] = True
        except Exception:
            print("Error detected in stage 1, Invalid data")
            data = {"error", "error in stage 1"}
        return data


class TransformStage():

    def __init__(self):
        print("Stage 2: Data transformation and enrichment")

    def process(self, data: Any) -> Any:
        pass


class OutputStage():

    def __init__(self):
        print("Stage 3: Output formatting and delivery")

    def process(self, data: Any) -> Any:
        pass


class JSONAdapter(ProcessingPipeline):
    def __init__(self, pipeline_id: str):
        super().__init__()
        self.pipeline_id = pipeline_id

    def process(self, data: Any) -> Union[str, Any]:
        data = {"raw_data": data, "type": "json"}
        for stage in self.stages:
            data = stage.process(data)
            if isinstance(data, dict) and "error" in data:
                break
        return data


class CSVAdapter(ProcessingPipeline):
    def __init__(self, pipeline_id: str):
        super().__init__()
        self.pipeline_id = pipeline_id

    def process(self, data: Any) -> Union[str, Any]:
        data = {"raw_data": data, "type": "csv"}
        for stage in self.stages:
            data = stage.process(data)
            if isinstance(data, dict) and "error" in data:
                break
        return data


class StreamAdapter(ProcessingPipeline):
    def __init__(self, pipeline_id: str):
        super().__init__()
        self.pipeline_id = pipeline_id

    def process(self, data: Any) -> Union[str, Any]:
        data = {"raw_data": data, "type": "stream"}
        for stage in self.stages:
            data = stage.process(data)
            if isinstance(data, dict) and "error" in data:
                break
        return data


class NexusManager():

    def __init__(self):
        self.pipelines: List[ProcessingPipeline] = []
        print("Initializing Nexus Manager...")
        print("Pipeline capacity: 1000 streams/second")

    def add_pipeline(self, pipeline: ProcessingPipeline):
        self.pipelines.append(pipeline)

    def process(self):
        for pipeline in self.pipelines:
            pipeline.process()


def main() -> None:
    print("=== CODE NEXUS - ENTERPRISE PIPELINE SYSTEM ===")
    try:
        manager = NexusManager()
        print("Creating Data Processing Pipeline...")
        stages: List[ProcessingStage] = [
            InputStage(),
            TransformStage(),
            OutputStage()
        ]
        pipelines: List[ProcessingPipeline] = [
              JSONAdapter("JSON_001"),
              CSVAdapter("CSV_001"),
              StreamAdapter("STREAM_001")
        ]
        for pipeline in pipelines:
            for stage in stages:
                pipeline.add_stage(stage)
            manager.add_pipeline(pipeline)
        manager.process()

    except Exception:
        pass


if __name__ == "__main__":
    main()
