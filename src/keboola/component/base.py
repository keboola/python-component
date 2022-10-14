import logging
import os
import json
from . import dao
from . import table_schema as ts
from abc import ABC, abstractmethod
from pathlib import Path
from typing import Optional, Dict
from .interface import CommonInterface

KEY_DEBUG = 'debug'


class ComponentBase(ABC, CommonInterface):
    def __init__(self, data_path_override: Optional[str] = None,
                 schema_path_override: Optional[str] = None,
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

        self.schema_folder_path = self._get_schema_folder_path(schema_path_override)

    @staticmethod
    def _get_default_data_path() -> str:
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

    def _get_schema_folder_path(self, schema_path_override: str = None) -> str:
        """
            Returns value of the schema_folder_path in case the schema_path_override variable is provided or
            the default schema_folder_path is found.

        """
        return schema_path_override or self._get_default_schema_folder_path()

    @staticmethod
    def _get_default_schema_folder_path() -> Optional[str]:
        """
             Finds the default schema_folder_path if it exists.

        """
        container_schema_dir = Path("./src/schemas/").absolute().as_posix()
        local_schema_dir = Path("./schemas").absolute().as_posix()
        if os.path.isdir(container_schema_dir):
            return container_schema_dir
        elif os.path.isdir(local_schema_dir):
            return local_schema_dir

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

    def create_out_table_definition_from_schema(self, table_schema: ts.TableSchema, is_sliced: bool = False,
                                                destination: str = '', incremental: bool = None,
                                                enclosure: str = '"', delimiter: str = ',',
                                                delete_where: dict = None) -> dao.TableDefinition:
        """
            Creates an out table definition using a defined table schema.
            This method uses the given table schema and generates metadata of the table. Along with the additional
            key word arguments it creates an out table definition.

            Args:
                table_schema : table of the schema for which a table definition will be created
                is_sliced: True if the full_path points to a folder with sliced tables
                destination: String name of the table in Storage.
                incremental: Set to true to enable incremental loading
                enclosure: str: CSV enclosure, by default "
                delimiter: str: CSV delimiter, by default ,
                delete_where: Dict with settings for deleting rows

            Returns:
                TableDefinition object initialized with all table metadata defined in a schema

        """
        table_metadata = self._generate_table_metadata(table_schema)
        return self.create_out_table_definition(name=table_schema.csv_name,
                                                columns=table_schema.field_names,
                                                primary_key=table_schema.primary_keys,
                                                table_metadata=table_metadata,
                                                is_sliced=is_sliced,
                                                destination=destination,
                                                incremental=incremental,
                                                enclosure=enclosure,
                                                delimiter=delimiter,
                                                delete_where=delete_where)

    def get_table_schema_by_name(self, schema_name: str,
                                 schema_folder_path: Optional[str] = None) -> ts.TableSchema:
        """
            The method finds a table schema JSON based on it's name in a defined schema_folder_path and generates
            a TableSchema object.

            Args:
                schema_name : name of the schema in the schema_folder_path. e.g. for schema in 'src/schemas/order.json'
                              schema_name is 'order'
                schema_folder_path : directory path to the schema folder, by default the schema folder is set at
                                     'src/schemas'
            Returns:
                TableSchema object initialized with all available table metadata


        """
        if not schema_folder_path:
            schema_folder_path = self.schema_folder_path
        self._validate_schema_folder_path(schema_folder_path)
        schema_dict = self._load_table_schema_dict(schema_name, schema_folder_path)
        return ts.init_table_schema_from_dict(schema_dict)

    @staticmethod
    def _load_table_schema_dict(schema_name: str, schema_folder_path: str) -> Dict:
        try:
            with open(os.path.join(schema_folder_path, f"{schema_name}.json"), 'r') as schema_file:
                json_schema = json.loads(schema_file.read())
        except FileNotFoundError as file_err:
            raise FileNotFoundError(
                f"Schema for corresponding schema name : {schema_name} is not found in the schema directory. "
                f"Make sure that '{schema_name}'.json "
                f"exists in the directory '{schema_folder_path}'") from file_err
        return json_schema

    @staticmethod
    def _validate_schema_folder_path(schema_folder_path: str):
        if not schema_folder_path or not os.path.isdir(schema_folder_path):
            raise FileNotFoundError("A schema folder path must be defined in order to create a out table definition "
                                    "from a schema. If a schema folder path is not defined, the schemas folder must be"
                                    " located in the 'src' directory of a component : src/schemas")

    def _generate_table_metadata(self, table_schema: ts.TableSchema) -> dao.TableMetadata:
        """
            Generates a TableMetadata object for the table definition using a TableSchema object.

        """
        table_metadata = dao.TableMetadata()
        if table_schema.description:
            table_metadata.add_table_description(table_schema.description)
        table_metadata.add_column_descriptions({field.name: field.description for field in table_schema.fields})
        table_metadata = self._add_field_data_types_to_table_metadata(table_schema, table_metadata)
        return table_metadata

    @staticmethod
    def _add_field_data_types_to_table_metadata(table_schema: ts.TableSchema,
                                                table_metadata: dao.TableMetadata) -> dao.TableMetadata:
        """
            Adds data types of all fields specified in a TableSchema object to a given TableMetadata object

        """
        for field in table_schema.fields:
            if field.base_type:
                table_metadata.add_column_data_type(field.name,
                                                    data_type=field.base_type,
                                                    nullable=field.nullable,
                                                    length=field.length,
                                                    default=field.default)
        return table_metadata
