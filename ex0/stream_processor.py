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
		the_sum = sum(data)
		if total > 0:
			average = the_sum / total
			return f"Processed {total} numeric values, sum={the_sum}, avg={average}"
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
			data.split(' ')
		except AttributeError:
			return False

		return True

	def format_output(self, result: str) -> str:
		return super().format_output(result)
		


class LogProcessor(DataProcessor):

	def process(self, data: Any) -> str:
		splited = data.split(':')
		type = splited[0]
		msg = splited[1]
		if type == "ERROR":
			if not msg:
				return (f"[ALERT] ERROR level detected: no message provided !")
			return (f"[ALERT] ERROR level detected:{msg}")
		elif type == "INFO":
			if not msg:
				return (f"[INFO] INFO level detected: no message provided !")
			return (f"[INFO] INFO level detected:{msg}")
		return "g"

	def validate(self, data: Any) -> bool:
		if type(data) is not str:
			return False
		if data.startswith("ERROR:") is True:
			return True
		if data.startswith("INFO:") is True:
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
		result =  text.process(string)
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


def processing_multiple_data() -> None:
	print("\n=== Polymorphic Processing Demo ===")
	print("Processing multiple data types through same interface...")
	list = [
		(NumericProcessor(), [1, 2, 3]),
		(TextProcessor(), "hello world!"),
		(LogProcessor(), "INFO: System ready")
	]
	result = 1
	for object, data in list:
		print(f"Result {result}: {object.process(data)}")
		result += 1
	print("\nFoundation systems online. Nexus ready for advanced streams.")



def main() -> None:
	print("=== CODE NEXUS - DATA PROCESSOR FOUNDATION ===")
	processing_numeric_processor()
	processing_text_processor()
	processing_log_processor()
	processing_multiple_data()


if __name__ == "__main__":
	main()
