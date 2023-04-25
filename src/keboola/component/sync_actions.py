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


@dataclass
class SyncActionResult(ABC):
    """
    Abstract base for sync action results
    """

    def __post_init__(self):
        """
         Right now the status is always success.
        In other cases exception is thrown and printed via stderr.

        """
        self.status = 'success'

    def __str__(self):
        # the None values / attributes will be ignored.
        dict_obj = dataclasses.asdict(self, dict_factory=lambda x: {k: v for (k, v) in x if
                                                                    v is not None})
        # hack to add default status
        if self.status:
            dict_obj['status'] = self.status
        return json.dumps(dict_obj)


# str base so it is serialised properly
class MessageType(str, Enum):
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
        # special case of element SyncActionResult with no status. (all other must contain {"status":true}
        self.status = None


def process_sync_action_result(result: Union[None, List[dict], dict, SyncActionResult, List[SyncActionResult]]) -> str:
    """
    Converts Sync Action result into valid string (expected by Sync Action).
    Args:
        result: Union[None, SyncActionResult, List[SyncActionResult]]

    Returns: str: Valid string representation of the Sync action result.

    """
    if isinstance(result, SyncActionResult):
        result_str = str(result)
    elif isinstance(result, list):
        result_str = f'[{", ".join([json.dumps(r) if isinstance(r, dict) else str(r) for r in result])}]'
    elif result is None:
        result_str = json.dumps({'status': 'success'})
    elif isinstance(result, dict):
        # for backward compatibility
        result_str = json.dumps(result)
    else:
        raise ValueError("Result of sync action must be either None or an instance of SyncActionResult "
                         "or a List[SyncActionResult]")
    return result_str
