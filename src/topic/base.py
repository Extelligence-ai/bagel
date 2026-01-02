"""An abstract base class for topic registries."""

import abc
from typing import Final

import pyarrow as pa

DESCRIPTION_KEY: Final[str] = "description"
UNITS_KEY: Final[str] = "units"
DEFAULT_KEY: Final[str] = "default"


class TopicNotFoundError(Exception):
    """Raised when a topic does not exist in the timeseries data source."""


class TopicRegistry(abc.ABC):
    """An abstract base class for topic registries.

    A topic registry acts as an interface to discover available topics and their
    schemas, descriptions, and other metadata.

    Note:
        Constructors of all subclasses must only accept primitive types (e.g.,
        str, int, bool). This ensures instances can be reliably serialized and
        recreated via dependency injection.

    """

    @abc.abstractmethod
    def available_topics(self, data_source: object) -> list[str]:
        """Return a list of available topics associated with the data source.

        Args:
            data_source (object): The timeseries data source instance.

        Returns:
            list[str]: A list of available topic names in the data source.

        """

    @abc.abstractmethod
    def native_type_name(self, topic: str, data_source: object) -> str:
        """Return the native type name for the given topic.

        Args:
            topic (str): The name of the topic.
            data_source (object): The timeseries data source instance.

        Returns:
            str: The native type name for the given topic.

        Raises:
            TopicNotFoundError: If the specified topic does not exist.

        """

    @abc.abstractmethod
    def message_count(self, topic: str, data_source: object) -> int | None:
        """Return the number of messages for the given topic, if applicable.

        Args:
            topic (str): The name of the topic.
            data_source (object): The timeseries data source instance.

        Returns:
            int | None: The number of messages for the topic. Returns `None` if the
                data source is an unbounded stream or if the count is unknown or
                not efficiently retrievable. Note: `None` indicates an unknown
                count, not a count of zero.

        Raises:
            TopicNotFoundError: If the specified topic does not exist.

        """

    @abc.abstractmethod
    def struct(self, topic: str, data_source: object) -> pa.StructType:
        """Return the PyArrow StructType for the given topic.

        Args:
            topic (str): The name of the topic.
            data_source (object): The timeseries data source instance.

        Returns:
            pa.StructType: The PyArrow StructType for the given topic.

        Raises:
            TopicNotFoundError: If the specified topic does not exist.

        Notes:
            Implementers should include metadata with "description" and "units"
            keys in each field of the returned `StructType`. This metadata provides
            essential context for LLMs.

            Example of a well-formed PyArrow Field with metadata:
            ```python
            pa.field(
                "x",
                pa.float64(),
                nullable=False,
                metadata={
                    "description": "device's position on the x-axis",
                    "units": "meters",
                }
            )
            ```

        """

    @abc.abstractmethod
    def describe(self, topic: str, data_source: object) -> str | None:
        """Return a human-readable description of the given topic.

        Args:
            topic (str): The name of the topic.
            data_source (object): The timeseries data source instance.

        Returns:
            str | None: A human-readable description of the given topic, or `None` if no
                description is available.

        Raises:
            TopicNotFoundError: If the specified topic does not exist.

        Notes:
            Implementers should provide a meaningful description for each topic.
            This description will be used to provide semantic context to LLMs.

            Example of a well-formed description:
            '''
            GPS data including latitude, longitude, altitude, speed, and timestamp information.
            '''

        """
