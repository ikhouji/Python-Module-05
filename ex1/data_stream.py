from abc import ABC, abstractmethod
from typing import Any, List, Dict, Union, Optional


class DataStream(ABC):

    def __init__(self, stream_id: Union[str, float, int]):
        super().__init__()
        self.stream_id = stream_id

    @abstractmethod
    def process_batch(self, data_batch: List[Any]) -> str:
        pass

    def filter_data(self, data_batch: List[Any],
                    criteria: Optional[str] = None) -> List[Any]:
        return data_batch

    def get_stats(self) -> Dict[str, Union[str, int, float]]:
        return {"stream_id": self.stream_id, "Type": "General Data"}


class SensorStream(DataStream):

    def __init__(self, stream_id: Union[str, float, int]):
        super().__init__(stream_id)

    def process_batch(self, data_batch: List[Any]) -> str:
        if isinstance(data_batch, list) is False:
            return "this is not a valid data !!!"
        count: int = 0
        temp: Union[int, float] = 0
        temp_count: int = 0
        average: Union[int, float] = 0
        try:
            for element in data_batch:
                splited = element.split(':')
                if splited[0] == "temp":
                    count += 1
                    temp += float(splited[1])
                    temp_count += 1
                else:
                    count += 1
            if temp_count > 0:
                average = temp / temp_count

            return (f"Sensor analysis: {count} readings processed,"
                    f" avg temp: {average}°C")

        except Exception:
            return "Error when processing Sensor data"

    def filter_data(self, data_batch: List[Any],
                    criteria: Optional[str] = None) -> List[Any]:
        if criteria is None:
            return data_batch
        filtered: List[str] = []
        if criteria == "critical":
            for element in data_batch:
                info = element.split(':')[0]
                value = float(element.split(':')[1])
                if info == "temp":
                    if value > 37:
                        filtered.append(element)

        elif criteria == "non-critical":
            for element in data_batch:
                info = element.split(':')[0]
                value = float(element.split(':')[1])
                if info == "temp":
                    if value <= 37:
                        filtered.append(element)
        return filtered

    def get_stats(self) -> Dict[str, Union[str, int, float]]:
        return {"stream_id": self.stream_id, "type": "Environmental Data"}


class TransactionStream(DataStream):

    def __init__(self, stream_id: Union[str, float, int]):
        super().__init__(stream_id)

    def process_batch(self, data_batch: List[Any]) -> str:
        if isinstance(data_batch, list) is False:
            return "this is not a valid data !!!"

        buy: int = 0
        sell: int = 0
        count: int = 0
        try:
            for element in data_batch:
                splited = element.split(':')
                count += 1
                if splited[0] == "buy":
                    buy += int(splited[1])
                elif splited[0] == "sell":
                    sell += int(splited[1])
                else:
                    return "Error when processing Transaction data"
        except Exception:
            return "Error when processing Transaction data"
        net: int = buy - sell
        return (f"Transaction analysis: {count} operations, net flow:"
                f" {net:+} units")

    def filter_data(self, data_batch: List[Any],
                    criteria: Optional[str] = None) -> List[Any]:
        if criteria is None:
            return data_batch
        filtered: List[str] = []
        if criteria == "large":
            for element in data_batch:
                transaction = element.split(':')[0]
                value = int(element.split(':')[1])
                if transaction == "buy" or transaction == "sell":
                    if value > 150:
                        filtered.append(element)

        elif criteria == "small":
            for element in data_batch:
                transaction = element.split(':')[0]
                value = transaction = element.split(':')[1]
                if transaction == "buy" or transaction == "sell":
                    if value <= 150:
                        filtered.append(element)

        return filtered

    def get_stats(self) -> Dict[str, Union[str, int, float]]:
        return {"stream_id": self.stream_id, "type": "Financial Data"}


class EventStream(DataStream):

    def __init__(self, stream_id: Union[str, float, int]):
        super().__init__(stream_id)

    def process_batch(self, data_batch: List[Any]) -> str:
        count: int = 0
        errors: int = 0
        if isinstance(data_batch, list) is False:
            return "this is not a valid data !!!"
        for element in data_batch:
            if element == "error":
                errors += 1
            count += 1
        return f"Event analysis: {count} events, {errors} error detected"

    def filter_data(self, data_batch: List[Any],
                    criteria: Optional[str] = None) -> List[Any]:
        if criteria is None:
            return data_batch
        filtered: List[str] = []
        for element in data_batch:
            if element == criteria:
                filtered.append(element)
        return filtered

    def get_stats(self) -> Dict[str, Union[str, int, float]]:
        return {"stream_id": self.stream_id, "type": "System Events"}


def initializing_sensor_stream(object: SensorStream) -> None:
    print("\nInitializing Sensor Stream...")
    info = object.get_stats()
    print("Stream ID: ", info['stream_id'], ", Type: ", info["type"], sep='')
    data_batch = ["temp:22.5", "humidity:65", "pressure:1013"]
    print("Processing sensor batch:", data_batch)
    print(object.process_batch(data_batch))


def initializing_transaction_stream(object: TransactionStream) -> None:
    print("\nInitializing Transaction Stream...")
    info = object.get_stats()
    print("Stream ID: ", info['stream_id'], ", Type: ", info["type"], sep='')
    data_batch = ["buy:100", "sell:150", "buy:75"]
    print("Processing transaction batch:", data_batch)
    print(object.process_batch(data_batch))


def initializing_event_stream(object: EventStream) -> None:
    print("\nInitializing Event Stream...")
    info = object.get_stats()
    print("Stream ID: ", info['stream_id'], ", Type: ", info["type"], sep='')
    data_batch = ["login", "error", "logout"]
    print("Processing event batch:", data_batch)
    print(object.process_batch(data_batch))


class StreamProcessor():

    def process_multiple_streams(self, streams: List) -> None:
        if isinstance(streams, list) is False:
            print("this is not a valid data !!!")
            return
        print("Batch 1 Results:")
        for element in streams:
            data = element[0].process_batch(element[1])
            splited = data.split(' ')
            print(f"- {splited[0]} data: {splited[2]}"
                  f" {splited[3].strip(',')} processed")


def process_multiple_streams() -> None:
    print("\n=== Polymorphic Stream Processing ===")
    print("Processing mixed stream types through unified interface...\n")
    sensor_data = ["humidity:65", "pressure:1013"]
    transaction_data = ["buy:100", "sell:150", "buy:75", "sell:75"]
    event_data = ["login", "error", "logout"]
    list = [(SensorStream("SENSOR_002"), sensor_data),
            (TransactionStream("TRANS_002"), transaction_data),
            (EventStream("EVENT_002"), event_data)
            ]
    stream_processor = StreamProcessor()
    stream_processor.process_multiple_streams(list)


def filter_multiple_data() -> None:
    print("\nStream filtering active: High-priority data only")
    sensor = SensorStream("SENSOR_003")
    sensor_data = ["temp:28.5", "temp:39", "pressure:1013", "temp:41.5"]
    s_criteria: str = "critical"
    s_len: int = len(sensor.filter_data(sensor_data, s_criteria))
    transaction = TransactionStream("TRANS_003")
    transaction_data = ["buy:100", "sell:150", "buy:200", "sell:75"]
    t_criteria: str = "large"
    t_len: int = len(transaction.filter_data(transaction_data, t_criteria))

    print(f"Filtered results: {s_len} {s_criteria} sensor alerts,"
          f" {t_len} {t_criteria} transaction")


if __name__ == "__main__":
    print("=== CODE NEXUS - POLYMORPHIC STREAM SYSTEM ===")
    sensor = SensorStream("SENSOR_001")
    initializing_sensor_stream(sensor)
    transaction = TransactionStream("TRANS_001")
    initializing_transaction_stream(transaction)
    event = EventStream("EVENT_001")
    initializing_event_stream(event)
    process_multiple_streams()
    filter_multiple_data()
    print("\nAll streams processed successfully. Nexus throughput optimal.")
