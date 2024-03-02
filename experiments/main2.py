import configparser

from dependency_injector.containers import DeclarativeContainer
from dependency_injector.providers import Factory
from abc import ABC, abstractmethod


# Define Data and Message (replace with your specific data format)
class Data:
    def __init__(self, value):
        self.value = value


class Message:
    def __init__(self, topic, data):
        self.topic = topic
        self.data = data


# Repository Interface
class DataRepository(ABC):
    @abstractmethod
    def get_data(self) -> Data:
        pass

    @abstractmethod
    def store_data(self, data: Data):
        pass


class IDataService(ABC):
    """Interface for Data Service operations.
    """

    @abstractmethod
    def get_data(self) -> Data:
        """
        Retrieves data from the underlying source.

        Returns:
        Data object containing retrieved data.
        """
        pass

    @abstractmethod
    def store_data(self, data: Data):
        """
        Stores data to the underlying destination.

        Args:
        data: The data object to be stored.
        """
        pass


# Concrete Data Service with Repository
class DataService(IDataService):
    def __init__(self, repository: DataRepository):
        self.repository = repository

    def get_data(self) -> Data:
        return self.repository.get_data()

    def store_data(self, data: Data):
        self.repository.store_data(data)


# Mock In-Memory Repository (replace with actual implementation)
class MockRepository(DataRepository):
    def __init__(self):
        self.data = None

    def get_data(self) -> Data:
        return self.data

    def store_data(self, data: Data):
        self.data = data


# Base Node Class
class Node(ABC):
    def __init__(self, services: DataService):
        self.services = services
        self.next_node = None

    @abstractmethod
    def process(self, data: Data):
        pass


# Node Logic Examples
class DoubleNode(Node):
    def __init__(self, services: DataService):
        super().__init__(services)

    def process(self, data: Data):
        data.value *= 2
        self.next_node.process(data)  # Assuming next_node is set


class PrintNode(Node):
    def process(self, data: Data):
        print(f"Processed data: {data.value}")
        self.services.store_data(data)  # Using injected service


# Dependency Injection Container
class Services(DeclarativeContainer):
    config = configparser.ConfigParser()  # Replace with actual configuration
    config.read("config.ini")

    data_repository = Factory(
        MockRepository
    )

    data_service = Factory(
        DataService,
        repository=data_repository
    )


if __name__ == "__main__":
    # Create container instance
    container = Services()

    # Resolve DataService instance from the factory
    resolved_data_service = container.data_service()  # Resolve the factory

    # Build the pipeline2 with resolved DataService
    start_node = DoubleNode(resolved_data_service)
    start_node.next_node = PrintNode(resolved_data_service)

    # Trigger pipeline2 execution
    start_node.process(Data(10))
