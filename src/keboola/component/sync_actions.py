"""
Module containing helpers to work with Sync Actions.
For more info see [Sync actions](https://developers.keboola.com/extend/common-interface/actions/).
"""

import dataclasses
import json
from abc import ABC
from dataclasses import dataclass
from enum import Enum
from typing import Union, List, Optional


def _convert_enum_value(obj):
    """
    Helper to get Enums value
    Args:
        obj:

    Returns:

    """
    if isinstance(obj, Enum):
        return obj.value
    return obj


@dataclass
class SyncActionResult(ABC):
    """
    Abstract base for sync action results
    """

    def __post_init__(self):
        """
         Right now the status is always success.
        In other cases exception is thrown and printed via stderr.
        Returns:

        """
        self.status = 'success'

    def __str__(self):
        dict_obj = dataclasses.asdict(self, dict_factory=lambda x: {k: _convert_enum_value(v) for (k, v) in x if
                                                                    v is not None})
        # hack to add default status
        if self.status:
            dict_obj['status'] = self.status
        return json.dumps(dict_obj)


class MessageType(Enum):
    SUCCESS = "success"
    INFO = "info"
    WARNING = "warning"
    DANGER = "danger"


@dataclass
class ValidationResult(SyncActionResult):
    message: str
    type: MessageType = MessageType.INFO


@dataclass
class SelectElement(SyncActionResult):
    """
    For select elements. Label is optional and value will be used
    """
    value: str
    label: Optional[str] = None

    def __post_init__(self):
        self.label = self.label or self.value
        # special case of element F with no status
        self.status = None


def process_sync_action_result(result: Union[None, dict, SyncActionResult, List[SyncActionResult]]):
    """
    Converts Sync Action result into valid string.
    Args:
        result: Union[None, SyncActionResult, List[SyncActionResult]]

    Returns:

    """
    if isinstance(result, SyncActionResult):
        result_str = str(result)
    elif isinstance(result, list):
        result_str = f'[{", ".join([str(r) for r in result])}]'
    elif result is None:
        result_str = json.dumps({'status': 'success'})
    elif isinstance(result, dict):
        # for backward compatibility
        result_str = json.dumps(result)
    else:
        raise ValueError("Result of sync action must be either None or an instance of SyncActionResult "
                         "or a List[SyncActionResult]")
    return result_str
