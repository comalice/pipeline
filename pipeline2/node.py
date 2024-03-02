import uuid
from abc import ABC, abstractmethod
from typing import Generic, TypeVar

T = TypeVar('T')
V = TypeVar('V')


class Node(ABC, Generic[T, V]):
    """A node in a data processing pipeline2."""

    # init method should take input and output types as arguments
    def __init__(self, input_type: T, output_type: V, is_output=False) -> None:
        self.input_type = input_type
        self.output_type = output_type
        self.is_output = is_output
        self.uuid = uuid.uuid4()
        self.visited = False

    def __repr__(self) -> str:
        return f"{self.__class__.__name__}({self.input_type}) -> {self.output_type}"

    @property
    def id(self) -> str:
        """Return the class name as well as a UUID."""
        return f"{self.__class__.__name__}-{self.uuid}"

    @abstractmethod
    def process(self, _input: T) -> V:
        pass
