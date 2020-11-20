import argparse
import json
import logging
import os

from .interface_dao import *


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


class CommonInterface:
    """
    A class handling standard tasks related to the
    [Keboola Common Interface](https://developers.keboola.com/extend/common-interface/)
    e.g. config load, validation, component state, I/O handling, I/O metadata and manifest files.

    It initializes the environment inject into the Docker container the KBC component runs in and abstracts the tasks
    related to the Common Interface interaction
    """

    def __init__(self, data_folder_path: str = None, log_level=logging.INFO, logging_type=None):
        """
        Initializes the CommonInterface environment. If the data_folder_path is not specified the folder
        is established in following order:

        - From provided argument if present: `-d` or `--data`
        - From environment variable if present (KBC_DATADIR)
        - Defaults to /data/ if none of the above is specified

        Args:
            data_folder_path (str): path to a data folder.
        """
        self.environment_variables = init_environment_variables()

        if not data_folder_path:
            data_folder_path = self._get_data_folder_from_context()

        self.data_folder_path = data_folder_path

        # init configuration / load config.json
        self.configuration = Configuration(data_folder_path)

    def _get_data_folder_from_context(self):
        # try to get from argument parameter

        # get from parameters
        argparser = argparse.ArgumentParser()
        argparser.add_argument(
            '-d',
            '--data',
            dest='data_dir',
            default='',
            help='Data directory'
        )
        # unknown is to ignore extra arguments
        args, unknown = argparser.parse_known_args()
        data_folder_path = args.data_dir

        if data_folder_path == '' and self.environment_variables.data_dir:
            data_folder_path = self.environment_variables.data_dir
        elif data_folder_path == '':
            data_folder_path = '/data/'

        return data_folder_path

    def write_table_manifest(
            self,
            file_name: str,
            destination: str = '',
            primary_key: List[str] = None,
            columns: List[str] = None,
            incremental: bool = None,
            table_metadata: TableMetadata = None,
            delete_where: str = None):
        """
        Write manifest for output table Manifest is used for
        the table to be stored in KBC Storage.

        Args:
            file_name: Local file path of the file with table data. Or empty string if workspace is used
            destination: String name of the table in Storage.
            primary_key: List with names of columns used for primary key.
            columns: List of columns for headless CSV files
            incremental: Set to true to enable incremental loading
            table_metadata: <.interface_dao.TableMetadata> object containing column and table metadata
            delete_where: Dict with settings for deleting rows
        """
        manifest = {}
        if destination:
            if isinstance(destination, str):
                manifest['destination'] = destination
            else:
                raise TypeError("Destination must be a string")
        if primary_key:
            if isinstance(primary_key, list):
                manifest['primary_key'] = primary_key
            else:
                raise TypeError("Primary key must be a list")
        if columns:
            if isinstance(columns, list):
                manifest['columns'] = columns
            else:
                raise TypeError("Columns must by a list")
        if incremental:
            manifest['incremental'] = True
        manifest['column_metadata'] = table_metadata.column_metadata
        manifest['metadata'] = table_metadata.table_metadata
        manifest = self._process_delete(manifest, delete_where)
        with open(file_name + '.manifest', 'w') as manifest_file:
            json.dump(manifest, manifest_file)

    @staticmethod
    def _process_delete(manifest, delete_where):
        """
        Process metadata as dictionary and returns modified manifest

        Args:
            manifest: Manifest dict
            delete_where: Dictionary of where condition specification

        Returns:
            Manifest dict
        """
        if delete_where:
            if 'column' in delete_where and 'values' in delete_where:
                if not isinstance(delete_where['column'], str):
                    raise TypeError("Delete column must be a string")
                if not isinstance(delete_where['values'], list):
                    raise TypeError("Delete values must be a list")
                op = delete_where['operator'] or 'eq'
                if (not op == 'eq') and (not op == 'ne'):
                    raise ValueError("Delete operator must be 'eq' or 'ne'")
                manifest['delete_where_values'] = delete_where['values']
                manifest['delete_where_column'] = delete_where['column']
                manifest['delete_where_operator'] = op
            else:
                raise ValueError("Delete where specification must contain "
                                 "keys 'column' and 'values'")
        return manifest

    # ### PROPERTIES
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


# ########## CONFIGURATION

class Configuration:
    """
    Class representing configuration file generated and read
    by KBC for docker applications
    See docs:
    https://developers.keboola.com/extend/common-interface/config-file/
    """

    def __init__(self, data_folder_path: str):
        """

        Args:
            data_folder_path (object):
        """
        self.config_data = []
        self.data_dir = data_folder_path

        try:
            with open(os.path.join(data_folder_path, 'config.json'), 'r') \
                    as config_file:
                self.config_data = json.load(config_file)
        except (OSError, IOError):
            raise ValueError(
                "Configuration file config.json not found, " +
                "verify that the data directory is correct." +
                "Dir: " + self.data_dir
            )

        self.parameters = self.config_data.get('parameters', {})
        self.image_parameters = self.config_data.get('image_parameters', {})
        self.action = self.config_data.get('action', {})
        self.workspace_credentials = self.config_data.get('authorization', {}).get('workspace', {})

    # ################ PROPERTIES
    @property
    def oauth_credentials(self) -> OauthCredentials:
        """
        Returns subscriptable class OauthCredentials

        Returns: OauthCredentials

        """
        auth = self.config_data.get('authorization', {})
        credentials = None
        if auth:
            credentials = OauthCredentials(
                id=auth.get("id", ''),
                created=auth.get("created", ''),
                data=auth.get("#data", ''),
                oauthVersion=auth.get("oauthVersion", ''),
                appKey=auth.get("appKey", ''),
                appSecret=auth.get("#appSecret", '')
            )
        return credentials

    @property
    def tables_input_mapping(self) -> List[TableInputMapping]:
        """
        List of table [input mappings](https://developers.keboola.com/extend/common-interface/config-file/#tables)

        Tables specified in the configuration file.

        Returns: List[TableInputMapping]

        """

        tables_defs = self.config_data.get('storage', {}).get('input', {}).get('tables', [])
        tables = []
        for table in tables_defs:
            if 'column_types' in table:
                # nested dataclass
                table['column_types'] = build_dataclass_from_dict(TableColumnTypes, table['column_types'])

            im = build_dataclass_from_dict(TableInputMapping, table)
            im.full_path = os.path.normpath(
                os.path.join(
                    self.data_dir,
                    'in',
                    'tables',
                    table['destination']
                )
            )
            tables.append(table)
        return tables

    @property
    def tables_output_mapping(self) -> List[TableOutputMapping]:
        """
        List of table [output mappings](https://developers.keboola.com/extend/common-interface/config-file/#tables)

        Get tables which are supposed to be returned when the application finishes. (from configuration[
        'storage'] section.
        Returns: List[TableOutputMapping]

        """
        tables_defs = self.config_data.get('storage', {}).get('output', {}).get('tables', [])
        tables = []
        for table in tables_defs:
            om = build_dataclass_from_dict(TableOutputMapping, table)
            tables.append(om)
        return tables

    @property
    def files_input_mapping(self) -> List[FileInputMapping]:
        """
        List of file [input mappings](https://developers.keboola.com/extend/common-interface/config-file/#files)

        Files specified in the configuration file (defined on component's input mapping). (from configuration[
        'storage'] section.
        Returns: List[FileInputMapping]

        """
        defs = self.config_data.get('storage', {}).get('output', {}).get('files', [])
        files = []
        for file in defs:
            om = build_dataclass_from_dict(FileInputMapping, file)
            files.append(om)
        return files

    @property
    def files_output_mapping(self) -> List[FileOutputMapping]:
        """
        List of file [output mappings](https://developers.keboola.com/extend/common-interface/config-file/#files)

        Get files which are supposed to be returned when the application finishes. (from configuration[
        'storage'] section.
        Returns:

        """
        defs = self.config_data.get('storage', {}).get('output', {}).get('files', [])
        files = []
        for file in defs:
            om = build_dataclass_from_dict(FileOutputMapping, file)
            files.append(om)
        return files
