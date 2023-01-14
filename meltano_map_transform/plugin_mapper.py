from __future__ import annotations

import datetime
import hashlib
import logging
from typing import Any, Callable
import os

from singer_sdk.helpers import _simpleeval as simpleeval
from singer_sdk.exceptions import MapExpressionError, StreamMapConfigError
from singer_sdk._singerlib.catalog import Catalog

from singer_sdk.mapper import (
    CustomStreamMap,
    PluginMapper,
    RemoveRecordTransform,
    StreamMapsDict
)



MAPPER_ELSE_OPTION = "__else__"
MAPPER_FILTER_OPTION = "__filter__"
MAPPER_SOURCE_OPTION = "__source__"
MAPPER_ALIAS_OPTION = "__alias__"
MAPPER_KEY_PROPERTIES_OPTION = "__key_properties__"
NULL_STRING = "__NULL__"

def md5(input: str) -> str:
    """Digest a string using MD5. This is a function for inline calculations.
    Args:
        input: String to digest.
    Returns:
        A string digested into MD5.
    """
    return hashlib.md5(input.encode("utf-8")).hexdigest()


class ExtrensibleMapper(PluginMapper):
    """Inline map tranformer."""

    def __init__(
        self,
        plugin_config: dict[str, StreamMapsDict],
        logger: logging.Logger,
        mapper_class: type[Any] = Any,
    ) -> None:
        """Initialize mapper.
        Args:
            plugin_config: TODO
            logger: TODO
        Raises:
            StreamMapConfigError: TODO
        """
        super().__init__(plugin_config, logger)

        self.mapper_class = mapper_class


    def register_raw_stream_schema(
        self, stream_name: str, schema: dict, key_properties: list[str] | None
    ) -> None:
        """Register a new stream as described by its name and schema.
        If stream has already been registered and schema or key_properties has changed,
        the older registration will be removed and replaced with new, updated mappings.
        Args:
            stream_name: The stream name.
            schema: The schema definition for the stream.
            key_properties: The key properties of the stream.
        Raises:
            StreamMapConfigError: If the configuration is invalid.
        """
        if stream_name in self.stream_maps:
            primary_mapper = self.stream_maps[stream_name][0]
            if (
                primary_mapper.raw_schema != schema
                or primary_mapper.raw_key_properties != key_properties
            ):
                # Unload/reset stream maps if schema or key properties have changed.
                self.stream_maps.pop(stream_name)

        if stream_name not in self.stream_maps:
            # The 0th mapper should be the same-named treatment.
            # Additional items may be added for aliasing or multi projections.
            self.stream_maps[stream_name] = [
                self.default_mapper_type(
                    stream_name,
                    schema,
                    key_properties,
                    flattening_options=self.flattening_options,
                )
            ]

        for stream_map_key, stream_def in self.stream_maps_dict.items():
            stream_alias: str = stream_map_key
            source_stream: str = stream_map_key
            if isinstance(stream_def, str) and stream_def != NULL_STRING:
                if stream_name == stream_map_key:
                    # TODO: Add any expected cases for str expressions (currently none)
                    pass

                raise StreamMapConfigError(
                    f"Option '{stream_map_key}:{stream_def}' is not expected."
                )

            if stream_def is None or stream_def == NULL_STRING:
                if stream_name != stream_map_key:
                    continue

                self.stream_maps[stream_map_key][0] = RemoveRecordTransform(
                    stream_alias=stream_map_key,
                    raw_schema=schema,
                    key_properties=None,
                    flattening_options=self.flattening_options,
                )
                logging.info(f"Set null tansform as default for '{stream_name}'")
                continue

            if not isinstance(stream_def, dict):
                raise StreamMapConfigError(
                    "Unexpected stream definition type. Expected str, dict, or None. "
                    f"Got '{type(stream_def).__name__}'."
                )

            if MAPPER_SOURCE_OPTION in stream_def:
                source_stream = stream_def.pop(MAPPER_SOURCE_OPTION)

            if source_stream != stream_name:
                # Not a match
                continue

            if MAPPER_ALIAS_OPTION in stream_def:
                stream_alias = stream_def.pop(MAPPER_ALIAS_OPTION)

            mapper = self.mapper_class(
                stream_alias=stream_alias,
                map_transform=stream_def,
                map_config=self.map_config,
                raw_schema=schema,
                key_properties=key_properties,
                flattening_options=self.flattening_options,
            )

            if source_stream == stream_map_key:
                # Zero-th mapper should be the same-keyed mapper.
                # Override the default mapper with this custom map.
                self.stream_maps[stream_name][0] = mapper
            else:
                # Additional mappers for aliasing and multi-projection:
                self.stream_maps[stream_name].append(mapper)