from typing import Any, List, Dict, Union, Optional
from abc import ABC, abstractmethod


class DataProcessor(ABC):

    @abstractmethod
    def process(self, data: Any) -> str:
        pass

    @abstractmethod
    def validate(self, data: Any) -> bool:
        pass

    def format_output(self, result: str) -> str:
        return f"Output: {result}"


class NumericProcessor(DataProcessor):

    def process(self, data: Any) -> str:
        total: int = 0
        the_sum: float = 0
        average: float = 0

        if type(data) is int or type(data) is float:
            return f"Processed {1} numeric values, sum={data}, avg={data}"
        total = len(data)
        suum = sum(data)
        if total > 0:
            average = the_sum / total
            return f"Processed {total} numeric values,sum={suum},avg={average}"
        else:
            return ("Error: you Enter an empty list")

    def validate(self, data: Any) -> bool:
        if type(data) is int or type(data) is float:
            return True
        if type(data) is not list:
            return False

        try:
            for number in data:
                number = number + 1
        except TypeError:
            return False
        return True

    def format_output(self, result: str) -> str:
        return super().format_output(result)


class TextProcessor(DataProcessor):

    def process(self, data: Any) -> str:
        lenght = len(data)
        if lenght == 0:
            return ("Error: you typed an empty string")
        wc = len(data.split(' '))

        return f"Processed text: {lenght} characters, {wc} words"

    def validate(self, data: Any) -> bool:
        try:
            data.split()
        except AttributeError:
            return False

        return True

    def format_output(self, result: str) -> str:
        return super().format_output(result)


class LogProcessor(DataProcessor):

    def process(self, data: Any) -> str:
        splited = data.split(':')
        type_log = splited[0]
        msg = splited[1]
        if type_log == "ERROR":
            return (f"[ALERT] ERROR level detected:{msg}")
        elif type_log == "INFO":
            return (f"[INFO] INFO level detected:{msg}")
        return "Error: "

    def validate(self, data: Any) -> bool:
        if type(data) is not str:
            return False
        splited_data = data.split(':')
        if len(splited_data) != 2:
            return False
        if splited_data[0] == "ERROR":
            if splited_data[1].strip():
                return True
        if splited_data[0] == "INFO":
            if splited_data[1].strip():
                return True

        return False

    def format_output(self, result: str) -> str:
        return super().format_output(result)


def processing_numeric_processor() -> None:
    print("\nInitializing Numeric Processor...")
    numeric = NumericProcessor()
    data = [1, 2, 3, 4, 5]
    print("Processing data:", data)
    if numeric.validate(data):
        print("Validation: Numeric data verified")
        result = numeric.process(data)
        output = numeric.format_output(result)
        print(output)
    else:
        print("Validation: Numeric data not verified !!!")


def processing_text_processor() -> None:
    print("\nInitializing Text Processor...")
    text = TextProcessor()
    string = "Hello Nexus World"
    print(f"Processing data: \"{string}\"")
    if text.validate(string) is True:
        print("Validation: Text data verified")
        result = text.process(string)
        output = text.format_output(result)
        print(output)
    else:
        print("Validation: Text data not verified !!!")


def processing_log_processor() -> None:
    print("\nInitializing Log Processor...")
    logs = LogProcessor()
    log = "ERROR: Connection timeout"
    print(f"Processing data: \"{log}\"")
    if logs.validate(log) is True:
        print("Validation: Log entry verified")
        result = logs.process(log)
        output = logs.format_output(result)
        print(output)
    else:
        print("Validation: Log entry not verified !!!")


def processing_multiple_data(list_data: List) -> None:
    print("\n=== Polymorphic Processing Demo ===")
    print("Processing multiple data types through same interface...")
    result = 1
    for object, data in list_data:
        try:
            print(f"Result {result}: {object.process(data)}")
            result += 1
        except Exception:
            print("Error: invalid data !!!")
    print("\nFoundation systems online. Nexus ready for advanced streams.")


def main() -> Optional[int]:
    print("=== CODE NEXUS - DATA PROCESSOR FOUNDATION ===")
    processing_numeric_processor()
    processing_text_processor()
    processing_log_processor()
    list_data: List[Union[tuple[DataProcessor, Any], Dict]] = [
        (NumericProcessor(), [1, 2, 3]),
        (TextProcessor(), "hello world!"),
        (LogProcessor(), "INFO: System ready")
    ]
    processing_multiple_data(list_data)
    return 0


if __name__ == "__main__":
    main()
