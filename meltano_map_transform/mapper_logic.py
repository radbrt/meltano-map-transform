from singer_sdk.helpers._flattening import FlatteningOptions
import os
from singer_sdk.helpers import _simpleeval as simpleeval
from typing import Any, Callable
import hashlib
import datetime

from singer_sdk.mapper import (
    CustomStreamMap,
)

def md5(input: str) -> str:
    """Digest a string using MD5. This is a function for inline calculations.
    Args:
        input: String to digest.
    Returns:
        A string digested into MD5.
    """
    return hashlib.md5(input.encode("utf-8")).hexdigest()


class ExtensibleStreamMap(CustomStreamMap):
    """A stream map that uses a custom environment to evaluate map expressions.

    This is a subclass of :class:`singer_sdk.mapper.CustomStreamMap` that uses a 
    custom environment to evaluate map expressions. 
    
    This custom environment is provided by the ``env`` argument to the constructor.
    """

    def __init__(
        self,
        stream_alias: str,
        map_config: dict,
        raw_schema: dict,
        key_properties: list[str] | None,
        map_transform: dict,
        flattening_options: FlatteningOptions | None,
    ) -> None:
        """Initialize mapper.
        Args:
            stream_alias: Stream name.
            map_config: Stream map configuration.
            raw_schema: Original stream's JSON schema.
            key_properties: Primary key of the source stream.
            map_transform: Dictionary of transformations to apply to the stream.
            flattening_options: Flattening options, or None to skip flattening.
        """
        super().__init__(
            stream_alias=stream_alias,
            map_config=map_config,
            raw_schema=raw_schema,
            key_properties=key_properties,
            map_transform=map_transform,
            flattening_options=flattening_options,
        )

    @property
    def functions(self) -> dict[str, Callable]:
        """Get availabale transformation functions.
        Returns:
            Functions which should be available for expression evaluation.
        """
        funcs: dict[str, Any] = simpleeval.DEFAULT_FUNCTIONS.copy()
        funcs["md5"] = md5
        funcs["datetime"] = datetime
        funcs["os"] = os
        return funcs

