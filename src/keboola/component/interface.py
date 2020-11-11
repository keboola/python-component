from dataclasses import dataclass

import os


# ################### DATA CLASSES

@dataclass
class EnvironmentVariables:
    """
    Dataclass for variables available in the docker environment
    https://developers.keboola.com/extend/common-interface/environment/#environment-variables
    """
    data_dir: str
    run_id: str
    project_id: str
    stack_id: str
    config_id: str
    component_id: str
    project_name: str
    token_id: str
    token_desc: str
    token: str
    url: str
    logger_addr: str
    logger_port: str


class CommonInterface:
    """
    A class handling standard tasks related to the
    [Keboola Common Interface](https://developers.keboola.com/extend/common-interface/)
    e.g. config load, validation, component state, I/O handling, I/O metadata and manifest files.

    It initializes the environment inject into the Docker container the KBC component runs in and abstracts the tasks
    related to the Common Interface interaction
    """

    def __init__(self, data_folder_path=None):

        self.environment_variables = self.init_environment_variables()

        if not data_folder_path and self.environment_variables.data_dir:
            data_folder_path = self.environment_variables.data_dir
        elif not data_folder_path:
            data_folder_path = '/data'

        self.data_folder_path = data_folder_path

        # init configuration / load config.json
        self.configuration = Configuration(data_folder_path)

    @property
    def out_tables_path(self):
        return os.path.join(self.data_folder_path, 'out', 'tables')

    @property
    def in_tables_path(self):
        return os.path.join(self.data_folder_path, 'in', 'tables')

    @property
    def out_files_path(self):
        return os.path.join(self.data_folder_path, 'out', 'files')

    @property
    def in_files_path(self):
        return os.path.join(self.data_folder_path, 'in', 'files')

    @staticmethod
    def init_environment_variables() -> EnvironmentVariables:
        """
        Initializes environment variables available in the docker environment
            https://developers.keboola.com/extend/common-interface/environment/#environment-variables

        Returns:
            EnvironmentVariables:
        """
        return EnvironmentVariables(data_dir=os.environ.get('KBC_DATADIR', None),
                                    run_id=os.environ.get('KBC_RUNID', None),
                                    project_id=os.environ.get('KBC_PROJECTID', None),
                                    stack_id=os.environ.get('KBC_STACKID', None),
                                    config_id=os.environ.get('KBC_CONFIGID', None),
                                    component_id=os.environ.get('KBC_COMPONENTID', None),
                                    project_name=os.environ.get('KBC_PROJECTNAME', None),
                                    token_id=os.environ.get('KBC_TOKENID', None),
                                    token_desc=os.environ.get('KBC_TOKENDESC', None),
                                    token=os.environ.get('KBC_TOKEN', None),
                                    url=os.environ.get('KBC_URL', None),
                                    logger_addr=os.environ.get('KBC_LOGGER_ADDR', None),
                                    logger_port=os.environ.get('KBC_LOGGER_PORT', None)
                                    )


class Configuration:
    """
    Class representing configuration file generated and read
    by KBC for docker applications
    See docs:
    https://developers.keboola.com/extend/common-interface/config-file/
    """

    def __init__(self, data_folder_path):
        """

        Args:
            data_folder_path (object):
        """
        pass
