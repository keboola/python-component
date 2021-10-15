import logging
import os
from abc import ABC, abstractmethod
from pathlib import Path
from typing import Optional

from .interface import CommonInterface

KEY_DEBUG = 'debug'


class ComponentBase(ABC, CommonInterface):
    def __init__(self, data_path_override: Optional[str] = None,
                 required_parameters: Optional[list] = None,
                 required_image_parameters: Optional[list] = None):
        """
        Base class for general Python components. Initializes the CommonInterface
        and performs configuration validation.

        For easier debugging the data folder is picked up by default from `../data` path,
        relative to working directory.

        If `debug` parameter is present in the `config.json`, the default logger is set to verbose DEBUG mode.

        Args:
            data_path_override:
                optional path to data folder that overrides the default behaviour (`KBC_DATADIR` environment variable).
                May be also specified by '-d' or '--data' commandline argument
            required_parameters:
                Optional[dict]: DEPRECATED required configuration parameters, if filled in,
                validation is done at constructor level
            required_image_parameters:
                Optional[dict]: DEPRECATED required image parameters, if filled in,
                validation is done at constructor level
        Raises:
            UserException - on config validation errors.
        """

        # for easier local project setup
        super().__init__(data_folder_path=self._get_data_folder_override_path(data_path_override))

        if required_parameters:
            self.validate_configuration_parameters(required_parameters)
        if required_image_parameters:
            self.validate_image_parameters(required_image_parameters)

        if self.configuration.parameters.get(KEY_DEBUG):
            self.set_debug_mode()

    def _get_default_data_path(self) -> str:
        """
        Returns default data_path, by default `../data` is used, relative to working directory.
        This helps with local development.

        Returns:

        """
        return Path(os.getcwd()).resolve().parent.joinpath('data').as_posix()

    def _get_data_folder_override_path(self, data_path_override: str = None) -> str:
        """
        Returns overridden value of the data_folder_path in case the data_path_override variable
        or `KBC_DATADIR` environment variable is defined. The `data_path_override` variable takes precendence.

        Returns null if override is not in place.

        Args:
            data_path_override:

        Returns:

        """
        data_folder_path = None
        if data_path_override:
            data_folder_path = data_path_override
        elif not os.environ.get('KBC_DATADIR'):
            data_folder_path = self._get_default_data_path()
        return data_folder_path

    @staticmethod
    def set_debug_mode():
        """
        Set the default logger to verbose mode.
        Returns:

        """
        logging.getLogger().setLevel(logging.DEBUG)

    @abstractmethod
    def run(self):
        """
        Main execution code of default run action.


        """
        pass

    def execute_action(self):
        """
        Executes action defined in the configuration. The action name must match implemented method.
        The default action is 'run'.
        """
        action = self.configuration.action
        if not action:
            logging.warning("No action defined in the configuration, using the default run action.")
            action = 'run'
        logging.info(f"Running action: {action}")
        try:
            action_method = getattr(self, action)
        except AttributeError as e:
            raise AttributeError(f"The defined action {action} is not implemented!") from e
        return action_method()
