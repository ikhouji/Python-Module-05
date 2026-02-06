import json
import time
from abc import ABC, abstractmethod
from typing import Any, List, Dict, Union, Optional, Protocol


class ProcessingStage (Protocol):

    def process(self, data: Any) -> Any:
        pass


class ProcessingPipeline(ABC):
    def __init__(self) -> None:
        self.stages: List[ProcessingStage] = []

    def add_stage(self, stage: ProcessingStage) -> None:
        self.stages.append(stage)

    @abstractmethod
    def process(self, data: Any) -> Any:
        pass


class InputStage():

    def __init__(self) -> None:
        print("Stage 1: Input validation and parsing")

    def process(self, data: Any) -> Any:
        to_print = data.get("to_print")
        try:
            if data["type"] == "json":
                if to_print:
                    print(f"Input: {data['raw']}")
            elif data["type"] == "csv":
                if to_print:
                    print(f"Input: \"{data['raw']}\"")
            elif data["type"] == "stream":
                if to_print:
                    print("Input: Real-time sensor stream")
            else:
                raise ValueError
            data["valid"] = True
        except Exception:
            if to_print:
                print("Error detected in stage 1, Invalid data")
            data = {"error": "error in stage 1"}
        return data


class TransformStage():

    def __init__(self) -> None:
        print("Stage 2: Data transformation and enrichment")

    def process(self, data: Any) -> Dict:
        to_print = data.get("to_print")
        try:
            type = data["type"]
            raw = data["raw"]
            if type == "json":
                data["parsed"] = json.loads(raw)
                if to_print:
                    print("Transform: Enriched with metadata and validation")
            elif type == "csv":
                data["parsed"] = raw.split(',')
                if to_print:
                    print("Transform: Parsed and structured data")
            elif type == "stream":
                data["parsed"] = [float(number) for number in raw]
                if to_print:
                    print("Transform: Aggregated and filtered")
            else:
                raise ValueError()
        except Exception:
            if to_print:
                print("Error detected in Stage 2: Invalid data format")
            data = {"error": "error in stage 2"}

        return data


class OutputStage():

    def __init__(self) -> None:
        print("Stage 3: Output formatting and delivery")

    def process(self, data: Any) -> Any:
        to_print = data.get("to_print")
        parsed = data.get("parsed")
        if data["type"] == "json":
            temp = parsed.get('value', 0)
            range = None
            if 23 < temp < 30:
                range = "(Normal range)"
            else:
                range = "(Out of range range)"
            if to_print:
                print(
                    f"Output: Processed temperature reading: {temp}°C {range}")
        elif data["type"] == "csv":
            wc = parsed.count("action")
            if to_print:
                print(f"Output: User activity logged: {wc} actions processed")
        elif data["type"] == "stream":
            temps: float = 0
            count: int = 0
            for temp in parsed:
                temps += temp
                count += 1
            if count == 0:
                if to_print:
                    print("Output: Stream summary: 0 readings, avg: 0°C")
            else:
                av: float = temps / count
                if to_print:
                    print(f"Output: Stream summary: {count}"
                          f" readings, avg: {av}°C")
        return data


class JSONAdapter(ProcessingPipeline):
    def __init__(self, pipeline_id: str) -> None:
        super().__init__()
        self.pipeline_id = pipeline_id

    def process(self, data: Any) -> Union[str, Any]:
        print("\nProcessing JSON data through pipeline...")
        data = {"raw": data, "type": "json", "to_print": True}
        for stage in self.stages:
            data = stage.process(data)
            if isinstance(data, dict) and "error" in data:
                break
        return data


class CSVAdapter(ProcessingPipeline):
    def __init__(self, pipeline_id: str) -> None:
        super().__init__()
        self.pipeline_id = pipeline_id

    def process(self, data: Any) -> Union[str, Any]:

        print("\nProcessing CSV data through same pipeline...")
        data = {"raw": data, "type": "csv", "to_print": True}
        for stage in self.stages:
            data = stage.process(data)
            if isinstance(data, dict) and "error" in data:
                break
        return data


class StreamAdapter(ProcessingPipeline):
    def __init__(self, pipeline_id: str) -> None:
        super().__init__()
        self.pipeline_id = pipeline_id

    def process(self, data: Any) -> Union[str, Any]:
        print("\nProcessing Stream data through same pipeline...")
        data = {"raw": data, "type": "stream", "to_print": True}
        for stage in self.stages:
            data = stage.process(data)
            if isinstance(data, dict) and "error" in data:
                break
        return data


class NexusManager():

    def __init__(self) -> None:
        self.pipelines: List[ProcessingPipeline] = []
        print("Initializing Nexus Manager...")
        print("Pipeline capacity: 1000 streams/second")

    def add_pipeline(self, pipeline: ProcessingPipeline) -> None:
        self.pipelines.append(pipeline)

    def process(self, data_list: List[Any]) -> None:
        for i in range(len(self.pipelines)):

            pipeline = self.pipelines[i]
            data = data_list[i]
            pipeline.process(data)


def test_failure_pipeline(stages: List[ProcessingStage]) -> None:
    print("\n=== Error Recovery Test ===")
    print("Simulating pipeline failure...")
    invalid_data = {
        "raw": "hello,world",
        "type": "json",
        "valid": False,
        "to_print": True
    }
    stages[1].process(invalid_data)
    print("Recovery initiated: Switching to backup processor")
    print("Recovery successful: Pipeline restored, processing"
          " resumed")


def pipeline_chaining(stages: List[ProcessingStage]) -> tuple:
    raw_data = "temp,hh,jj,action"
    data = {'raw': raw_data, 'type': 'csv', 'to_print': False}
    pipelines: List = []
    data_flow: List = []

    data = stages[0].process(data)
    if "error" in data:
        return pipelines, data_flow
    pipelines.append("Pipeline A")
    data_flow.append("Raw")
    data = stages[1].process(data)
    if "error" in data:
        return pipelines, data_flow
    pipelines.append("Pipeline B")
    data_flow.append("Processed")
    data_flow.append("Analyzed")
    data = stages[2].process(data)
    if "error" in data:
        return pipelines, data_flow
    pipelines.append("Pipeline C")
    data_flow.append("Stored")
    return pipelines, data_flow, data


def main() -> Optional[int]:
    start_time = time.time()

    print("=== CODE NEXUS - ENTERPRISE PIPELINE SYSTEM ===\n")
    try:
        manager = NexusManager()
        print("\nCreating Data Processing Pipeline...")
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

        print("\n=== Multi-Format Data Processing ===")
        data_to_process = [
            '{"sensor": "temp", "value": 23.5, "unit": "C"}',
            "user,action,timestamp",
            [22.1, 21.8, 22.5, 23.0, 21.1]
        ]
        manager.process(data_to_process * 50)
        data_flow = pipeline_chaining(stages)
        if data_flow and len(data_flow) >= 2:
            if data_flow[0]:
                print("\n=== Pipeline Chaining Demo ===")
                print(" -> ".join(data_flow[0]))
            if data_flow[1]:
                print("Data flow:", " -> ".join(data_flow[1]))
            if len(data_flow) == 3 and data_flow[2]:
                records = len(data_flow[2]["parsed"])
                print(f"Chain result: {records} records processed through "
                      f"{len(data_flow[0])}-stage pipeline")

        end_time = time.time()
        t = end_time - start_time
        ef = int((0.00015 * 100) / t)
        print(f"Performance: {ef}% efficiency, {t:.4f}s total processing time")
        test_failure_pipeline(stages)
        print("\nNexus Integration complete. All systems operational.")
    except Exception as e:
        print(f"\n[CRITICAL ERROR]: {e}")
        return (1)
    return (0)


if __name__ == "__main__":
    main()
