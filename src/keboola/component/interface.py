import glob

import argparse
import json
import logging
import os
import sys
from pathlib import Path
from pygelf import GelfUdpHandler, GelfTcpHandler

from .dao import *


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
    related to the Common Interface interaction.

    Attributes:
        data_folder_path (str):
            Full path to the /data folder

        configuration (Configuration):
            Configuration object with initialized configuration, handling tasks related to
            config manipulation.

    """
    LOGGING_TYPE_STD = 'std'
    LOGGING_TYPE_GELF = 'gelf'

    def __init__(self, data_folder_path: str = None, log_level=logging.INFO, logging_type=None):
        """
        Initializes the CommonInterface environment. If the data_folder_path is not specified the folder
        is established in following order:

        - From provided argument if present: `-d` or `--data`
        - From environment variable if present (KBC_DATADIR)
        - Defaults to /data/ if none of the above is specified

        Args:
            data_folder_path (str): path to a data folder.
            log_level (int): logging.INFO or logging.DEBUG
            logging_type (str): optional 'std' or 'gelf', if left empty determined automatically
        """
        self.environment_variables = init_environment_variables()

        # init logging
        logging_type_inf = CommonInterface.LOGGING_TYPE_GELF if os.getenv('KBC_LOGGER_ADDR',
                                                                          None) else CommonInterface.LOGGING_TYPE_STD
        if not logging_type:
            logging_type = logging_type_inf

        if logging_type == CommonInterface.LOGGING_TYPE_STD:
            self.set_default_logger(log_level)
        elif logging_type == CommonInterface.LOGGING_TYPE_GELF:
            self.set_gelf_logger(log_level)

        # init data folder
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

    # ================================= Logging ==============================
    @staticmethod
    def set_default_logger(log_level: int = logging.INFO):  # noqa: E301
        """
        Sets default console logger.

        Args:
            log_level: logging level, default: 'logging.INFO'

        Returns:
            Logger object

        """

        class InfoFilter(logging.Filter):
            def filter(self, rec):
                return rec.levelno in (logging.DEBUG, logging.INFO)

        hd1 = logging.StreamHandler(sys.stdout)
        hd1.addFilter(InfoFilter())
        hd2 = logging.StreamHandler(sys.stderr)
        hd2.setLevel(logging.WARNING)

        logging.getLogger().setLevel(log_level)
        # remove default handler
        for h in logging.getLogger().handlers:
            logging.getLogger().removeHandler(h)
        logging.getLogger().addHandler(hd1)
        logging.getLogger().addHandler(hd2)

        logger = logging.getLogger()
        return logger

    @staticmethod
    def set_gelf_logger(log_level: int = logging.INFO, transport_layer='TCP', stdout=False):  # noqa: E301
        """
        Sets gelf console logger. Handler for console output is not included by default,
        for testing in non-gelf environments use stdout=True.

        Args:
            log_level: logging level, default: 'logging.INFO'
            transport_layer: 'TCP' or 'UDP', default:'UDP
            stdout: if set to True, Stout handler is also included

        Returns: Logger object
        """
        # remove existing handlers
        for h in logging.getLogger().handlers:
            logging.getLogger().removeHandler(h)
        if stdout:
            CommonInterface.set_default_logger(log_level)

        # gelf handler setup
        host = os.getenv('KBC_LOGGER_ADDR', 'localhost')
        port = os.getenv('KBC_LOGGER_PORT', 12201)
        if transport_layer == 'TCP':
            gelf = GelfTcpHandler(host=host, port=port)
        elif transport_layer == 'UDP':
            gelf = GelfUdpHandler(host=host, port=port)
        else:
            raise ValueError(F'Unsupported gelf transport layer: {transport_layer}. Choose TCP or UDP')

        logging.getLogger().setLevel(log_level)
        logging.getLogger().addHandler(gelf)

        logger = logging.getLogger()
        return logger

    def get_state_file(self) -> dict:
        """

        Returns dict representation of state file or nothing if not present

        Returns:
            dict:

        """
        logging.info('Loading state file..')
        state_file_path = os.path.join(self.data_folder_path, 'in', 'state.json')
        if not os.path.isfile(state_file_path):
            logging.info('State file not found. First run?')
            return {}
        try:
            with open(state_file_path, 'r') \
                    as state_file:
                return json.load(state_file)
        except (OSError, IOError):
            raise ValueError(
                "State file state.json unable to read "
            )

    def write_state_file(self, state_dict: dict):
        """
        Stores [state file](https://developers.keboola.com/extend/common-interface/config-file/#state-file).
        Args:
            state_dict (dict):
        """
        if not isinstance(state_dict, dict):
            raise TypeError('Dictionary expected as a state file datatype!')

        with open(os.path.join(self.configuration.data_dir, 'out', 'state.json'), 'w+') as state_file:
            json.dump(state_dict, state_file)

    def get_input_tables_definitions(self, orphaned_manifests=False) -> List[TableDefinition]:
        """
        Return TableDefinition objects by scanning the `data/in/tables` folder.

        The TableDefinition will contain full path of the source file, it's name and manifest (if present). It also
        provides methods for updating the manifest metadata.

        By default, orphaned manifests are skipped.


        See Also: keboola.component.dao.TableDefinition

        Args:
            orphaned_manifests (bool): If True, manifests without corresponding files are fetched. This is useful in
            in scenarios where [workspaces exchange](
            https://developers.keboola.com/extend/common-interface/folders/#exchanging-data-via-workspace) is used
            e.g. when only manifest files are present in the `data/in/tables` folder.

        Returns: List[TableDefinition]

        """

        table_files = [f for f in glob.glob(self.tables_in_path + "/**", recursive=False) if
                       not f.endswith('.manifest')]
        table_defs = list()
        for t in table_files:
            is_sliced = False
            manifest = dict()
            p = Path(t)
            if Path(t + '.manifest').exists():
                manifest = json.load(open(t + '.manifest'))

            if p.is_dir() and manifest:
                is_sliced = True
            elif p.is_dir() and not manifest:
                # skip folders that do not have matching manifest
                logging.warning(f'Folder {t} does not have matching manifest, it will be ignored!')
                continue

            table_defs.append(TableDefinition(full_path=t, name=p.name, is_sliced=is_sliced, manifest=manifest))

        if orphaned_manifests:
            files_w_manifest = [t.full_path for t in table_defs]
            manifest_files = [f for f in glob.glob(self.tables_in_path + "/**.manifest", recursive=False)
                              if Path(t.full_path).name not in files_w_manifest]
            for t in manifest_files:
                p = Path(t)
                manifest = json.load(open(t))

                if p.is_dir():
                    # skip folders that do not have matching manifest
                    logging.warning(f'Manifest {t} is folder,s skipping!')
                    continue

                table_defs.append(TableDefinition(full_path=None, name=p.stem, is_sliced=False, manifest=manifest))
        return table_defs

    @staticmethod
    def build_manifest(destination: str = '',
                       primary_key: List[str] = None,
                       columns: List[str] = None,
                       incremental: bool = None,
                       table_metadata: TableMetadata = None,
                       delete_where: str = None) -> dict:
        """
        Create output table manifest.

        See Also: write_table_manifest()

        Args:
            destination: String name of the table in Storage.
            primary_key: List with names of columns used for primary key.
            columns: List of columns for headless CSV files
            incremental: Set to true to enable incremental loading
            table_metadata: <.dao.TableMetadata> object containing column and table metadata
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
        manifest = CommonInterface._process_delete(manifest, delete_where)
        return manifest

    @staticmethod
    def write_table_manifest(
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

        ** Usage:**

        ```python
        from keboola.component import CommonInterface
        from keboola.component import dao

        ci = CommonInterface()
        tm = dao.TableMetadata()
        tm.add_table_description("My new table")

        ci.write_table_manifest(filename= os.path.join(ci.tables_out_path,'mytable.csv'
                                , incremental = True
                                , table_metadata = tm
                                )
        ```


        Args:
            file_name: Local file path of the file with table data. Or empty string if workspace is used
            destination: String name of the table in Storage.
            primary_key: List with names of columns used for primary key.
            columns: List of columns for headless CSV files
            incremental: Set to true to enable incremental loading
            table_metadata: <.dao.TableMetadata> object containing column and table metadata
            delete_where: Dict with settings for deleting rows
        """
        manifest = CommonInterface.build_manifest(file_name,
                                                  destination,
                                                  primary_key,
                                                  columns,
                                                  incremental,
                                                  table_metadata,
                                                  delete_where)
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

    # TODO: refactor the validate config so it's more userfriendly
    """
        - Support for nested params?
        - 
    """

    # def validate_config(self, mandatory_params=None):
    #     """
    #             Validates config parameters based on provided mandatory parameters.
    #             All provided parameters must be present in config to pass.
    #             ex1.:
    #             par1 = 'par1'
    #             par2 = 'par2'
    #             mandatory_params = [par1, par2]
    #             Validation will fail when one of the above parameters is not found
    #
    #             Two levels of nesting:
    #             Parameters can be grouped as arrays par3 = [groupPar1, groupPar2]
    #             => at least one of the pars has to be present
    #             ex2.
    #             par1 = 'par1'
    #             par2 = 'par2'
    #             par3 = 'par3'
    #             groupPar1 = 'groupPar1'
    #             groupPar2 = 'groupPar2'
    #             group1 = [groupPar1, groupPar2]
    #             group3 = [par3, group1]
    #             mandatory_params = [par1, par2, group1]
    #
    #             Folowing logical expression is evaluated:
    #             Par1 AND Par2 AND (groupPar1 OR groupPar2)
    #
    #             ex3
    #             par1 = 'par1'
    #             par2 = 'par2'
    #             par3 = 'par3'
    #             groupPar1 = 'groupPar1'
    #             groupPar2 = 'groupPar2'
    #             group1 = [groupPar1, groupPar2]
    #             group3 = [par3, group1]
    #             mandatory_params = [par1, par2, group3]
    #
    #             Following logical expression is evaluated:
    #             par1 AND par2 AND (par3 OR (groupPar1 AND groupPar2))
    #             """
    #     if not mandatory_params:
    #         mandatory_params = []
    #     return self.validate_parameters(self.cfg_params, mandatory_params, 'config parameters')
    #
    # def validate_image_parameters(self, mandatory_params):
    #     """
    #             Validates image parameters based on provided mandatory parameters.
    #             All provided parameters must be present in config to pass.
    #             ex1.:
    #             par1 = 'par1'
    #             par2 = 'par2'
    #             mandatory_params = [par1, par2]
    #             Validation will fail when one of the above parameters is not found
    #
    #             Two levels of nesting:
    #             Parameters can be grouped as arrays par3 = [groupPar1, groupPar2]
    #             => at least one of the pars has to be present
    #             ex2.
    #             par1 = 'par1'
    #             par2 = 'par2'
    #             par3 = 'par3'
    #             groupPar1 = 'groupPar1'
    #             groupPar2 = 'groupPar2'
    #             group1 = [groupPar1, groupPar2]
    #             group3 = [par3, group1]
    #             mandatory_params = [par1, par2, group1]
    #
    #             Folowing logical expression is evaluated:
    #             Par1 AND Par2 AND (groupPar1 OR groupPar2)
    #
    #             ex3
    #             par1 = 'par1'
    #             par2 = 'par2'
    #             par3 = 'par3'
    #             groupPar1 = 'groupPar1'
    #             groupPar2 = 'groupPar2'
    #             group1 = [groupPar1, groupPar2]
    #             group3 = [par3, group1]
    #             mandatory_params = [par1, par2, group3]
    #
    #             Following logical expression is evaluated:
    #             par1 AND par2 AND (par3 OR (groupPar1 AND groupPar2))
    #             """
    #     return self.validate_parameters(self.image_params, mandatory_params, 'image/stack parameters')
    #
    # def validate_parameters(self, parameters, mandatory_params, _type):
    #     """
    #     Validates provided parameters based on provided mandatory parameters.
    #     All provided parameters must be present in config to pass.
    #     ex1.:
    #     par1 = 'par1'
    #     par2 = 'par2'
    #     mandatory_params = [par1, par2]
    #     Validation will fail when one of the above parameters is not found
    #
    #     Two levels of nesting:
    #     Parameters can be grouped as arrays par3 = [groupPar1, groupPar2] => at least one of the pars has to be
    #     present
    #     ex2.
    #     par1 = 'par1'
    #     par2 = 'par2'
    #     par3 = 'par3'
    #     groupPar1 = 'groupPar1'
    #     groupPar2 = 'groupPar2'
    #     group1 = [groupPar1, groupPar2]
    #     group3 = [par3, group1]
    #     mandatory_params = [par1, par2, group1]
    #
    #     Folowing logical expression is evaluated:
    #     Par1 AND Par2 AND (groupPar1 OR groupPar2)
    #
    #     ex3
    #     par1 = 'par1'
    #     par2 = 'par2'
    #     par3 = 'par3'
    #     groupPar1 = 'groupPar1'
    #     groupPar2 = 'groupPar2'
    #     group1 = [groupPar1, groupPar2]
    #     group3 = [par3, group1]
    #     mandatory_params = [par1, par2, group3]
    #
    #     Following logical expression is evaluated:
    #     par1 AND par2 AND (par3 OR (groupPar1 AND groupPar2))
    #     """
    #     missing_fields = []
    #     for par in mandatory_params:
    #         if isinstance(par, list):
    #             missing_fields.extend(self._validate_par_group(par, parameters))
    #         elif not parameters.get(par):
    #             missing_fields.append(par)
    #
    #     if missing_fields:
    #         raise ValueError(
    #             'Missing mandatory {} fields: [{}] '.format(_type, ', '.join(missing_fields)))
    #
    # def _validate_par_group(self, par_group, parameters):
    #     missing_fields = []
    #     is_present = False
    #     for par in par_group:
    #         if isinstance(par, list):
    #             missing_subset = self._get_par_missing_fields(par, parameters)
    #             missing_fields.extend(missing_subset)
    #             if not missing_subset:
    #                 is_present = True
    #
    #         elif parameters.get(par):
    #             is_present = True
    #         else:
    #             missing_fields.append(par)
    #     if not is_present:
    #         return missing_fields
    #     else:
    #         return []
    #
    # def _get_par_missing_fields(self, mand_params, parameters):
    #     missing_fields = []
    #     for par in mand_params:
    #         if not parameters.get(par):
    #             missing_fields.append(par)
    #     return missing_fields

    # ### PROPERTIES
    @property
    def tables_out_path(self):
        return os.path.join(self.data_folder_path, 'out', 'tables')

    @property
    def tables_in_path(self):
        return os.path.join(self.data_folder_path, 'in', 'tables')

    @property
    def files_out_path(self):
        return os.path.join(self.data_folder_path, 'out', 'files')

    @property
    def files_in_path(self):
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
