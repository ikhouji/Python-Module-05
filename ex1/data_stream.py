from abc import ABC, abstractmethod
from typing import Any, List, Dict, Union, Optional


class DataStream(ABC):

	@abstractmethod
	def process_batch(self, data_batch: List[Any]) -> str:
		pass

	def filter_data(self, data_batch: List[Any], criteria: Optional[str] = None) -> List[Any]:
		pass

	def get_stats(self) -> Dict[str, Union[str, int, float]]:
		pass

class SensorStream(DataStream):
	
	def process_batch(self, data_batch: List[Any]) -> str:
		pass

	def filter_data(self, data_batch: List[Any], criteria: Optional[str] = None) -> List[Any]:
		pass

	def get_stats(self) -> Dict[str, Union[str, int, float]]:
		pass

class TransactionStream(DataStream):

	def process_batch(self, data_batch: List[Any]) -> str:
		pass

	def filter_data(self, data_batch: List[Any], criteria: Optional[str] = None) -> List[Any]:
		pass

	def get_stats(self) -> Dict[str, Union[str, int, float]]:
		pass

class EventStream(DataStream):

	def process_batch(self, data_batch: List[Any]) -> str:
		pass

	def filter_data(self, data_batch: List[Any], criteria: Optional[str] = None) -> List[Any]:
		pass

	def get_stats(self) -> Dict[str, Union[str, int, float]]:
		pass