import dataclasses
from dataclasses import dataclass
from enum import Enum

from typing import List, Union, Dict


@dataclass
class SubscriptableDataclass:
    """
    Helper class to make dataclasses subscriptable
    """

    def __getitem__(self, index):
        return getattr(self, index)


# ############ COMMON INTERFACE
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


class SupportedDataTypes(Enum):
    """
    Enum of [supported datatypes](https://help.keboola.com/storage/tables/data-types/)
    """
    STRING = 'STRING'
    INTEGER = 'INTEGER'
    NUMERIC = 'NUMERIC'
    FLOAT = 'FLOAT'
    BOOLEAN = 'BOOLEAN'
    DATE = 'DATE'
    TIMESTAMP = 'TIMESTAMP'

    @classmethod
    def list(cls):
        return list(map(lambda c: c.value, cls))

    @classmethod
    def is_valid_type(cls, data_type: str):
        return data_type in cls.list()


class KBCMetadataKeys(Enum):
    base_data_type = 'KBC.datatype.basetype'
    data_type_nullable = 'KBC.datatype.nullable'
    data_type_length = 'KBC.datatype.length'
    data_type_default = 'KBC.datatype.default'
    description = 'KBC.description'
    created_by_component = 'KBC.createdBy.component.id'
    last_updated_by_component = 'KBC.lastUpdatedBy.component.id'


class TableMetadata:
    """
    Abstraction of metadata and table_metadata than can be provided within the manifest file. This is useful for
    creation
    of table/column descriptions, assigning column base types etc. without knowing the complexity
    of the json object and the internal KBC metadata keys.

    Example:

        ```python

        tm = TableMetadata()

        # or alternatively load from existing manifest
        # tm = TableMetadata(manifest_dict)

        # add column types
        tm.add_column_types({"column_a":"INTEGER", "column_b":SupportedDataTypes.BOOLEAN.value})

        # add table description
        tm.add_table_description("desc")

        # add column description
        tm.add_column_descriptions({"column_a":"Integer columns", "column_b":"my boolean test"})

        # add arbitrary table metadata
        tm.add_table_metadata("my_arbitrary_key","some value")

        # update manifest
        manifest = {}
        manifest['metadata'] = tm.get_table_metadata_for_manifest()
        manifest['column_metadata'] = tm.get_column_metadata_for_manifest()
        ```


    """

    def __init__(self, manifest: dict = None):
        """

        Args:
            manifest (dict): Existing manifest file
        """
        self.table_metadata = dict()
        self.column_metadata = dict()
        if manifest:
            self.load_table_metadata_from_manifest(manifest)

    def load_table_metadata_from_manifest(self, manifest: dict):
        """
        Load metadata from manifest file.

        Args:
            manifest:

        Returns:TableMetadata

        """
        # column metadata
        for column, metadata_list in manifest.get('column_metadata', {}):
            for metadata in metadata_list:
                if not metadata.get('key') and metadata.get('value'):
                    continue
                key = metadata['key']
                value = metadata['value']
                self.add_column_metadata(column, key, value)

        # table metadata
        for metadata in manifest.get('metadata', []):
            if not metadata.get('key') and metadata.get('value'):
                continue
            key = metadata['key']
            value = metadata['value']
            self.add_table_metadata(key, value)

    def get_table_metadata_for_manifest(self) -> List[dict]:
        """
        Returns table metadata list as required by the
        [manifest format](https://developers.keboola.com/extend/common-interface/manifest-files/#dataintables-manifests)

        e.g.
        tm = TableMetadata()
        manifest['metadata'] = tm.table_metadata

        Returns: List[dict]

        """
        final_metadata_list = [{'key': key,
                                'value': self.table_metadata[key]}
                               for key in self.table_metadata]

        return final_metadata_list

    def get_column_metadata_for_manifest(self) -> dict:
        """
                Returns column metadata dict as required by the
                [manifest format](https://developers.keboola.com/extend/common-interface/manifest-files/#dataintables
                -manifests)

                e.g.
                tm = TableMetadata()
                manifest['column_metadata'] = tm.column_metadata

                Returns: dict

        """
        final_column_metadata = dict()

        # collect unique metadata keys
        for column in self.column_metadata:
            column_metadata_dicts = self.column_metadata[column]
            if not final_column_metadata.get(column):
                final_column_metadata[column] = list()

            column_metadata = [{'key': key,
                                'value': column_metadata_dicts[key]} for key in
                               column_metadata_dicts[column]]
            final_column_metadata[column].extend(column_metadata)

        return final_column_metadata

    @property
    def table_description(self) -> str:
        """
        Returns table description (KBC.description)

        Returns: str

        """
        return self.table_metadata.get(KBCMetadataKeys.description.value)

    @property
    def column_datatypes(self) -> dict:
        """
        Return dictionary of column base datatypes
        e.g. {"col1name":"basetype"}

        Returns: dict e.g. {"col1name":"basetype"}

        """

        return self.get_columns_metadata_by_key(KBCMetadataKeys.base_data_type.value)

    @property
    def column_descriptions(self) -> dict:
        """
        Return dictionary of column descriptions
        e.g. {"col1name":"desc"}

        Returns: dict e.g. {"col1name":"desc"}

        """

        return self.get_columns_metadata_by_key(KBCMetadataKeys.description.value)

    def get_columns_metadata_by_key(self, metadata_key) -> dict:
        """
        Returns all columns with specified metadata_key as dictionary of column:metadata_key pairs
        e.g. {"col1name":"value_of_metadata_with_the_key"}

        Returns: dict e.g. {"col1name":"value_of_metadata_with_the_key"}

        """
        column_types = dict()
        for col in self.column_metadata:
            if col.get(metadata_key):
                column_types[col] = col[metadata_key]

        return column_types

    def add_column_descriptions(self, column_descriptions: dict):
        """
                Add column description metadata. It will be shown in the KBC Storage UI.

                Args:
                    column_descriptions: dict -> {"colname":"description"}

                """
        for col in column_descriptions:
            self.add_column_metadata(col, KBCMetadataKeys.description.value, column_descriptions[col])

    def add_column_data_types(self, column_types: Dict[str, Union[SupportedDataTypes, str]]):
        """
        Add column types metadata. Note that only supported datatypes
        (<keboola.component.dao.ColumnDataTypes>) may be provided. The value accepts either instance of ColumnDataTypes
        or a valid string.

        Args:
            column_types (Dict[str, Union[SupportedDataTypes, str]]): dict -> {"colname":"datatype"}

        Raises:
            ValueError when the provided data type value is not recognized
        """

        for col in column_types:
            self.add_column_data_type(col, column_types[col])

    def add_column_data_type(self, column: str, data_type: Union[SupportedDataTypes, str], nullable: bool = False,
                             length: str = None, default=None):
        """
        Add single column data type
        Args:
            column (str): name of the column
            data_type (Union[SupportedDataTypes, str]): Either instance of ColumnDataTypes enum or a valid string
            nullable (bool): Is column nullable? KBC input mapping converts empty values to NULL
            length (str): Column length when applicable e.g. 39,8, 4000
            default: Default value

        Raises:
            ValueError when the provided data_type is not recognized

        """
        if isinstance(data_type, SupportedDataTypes):
            base_type = data_type.value
        else:
            self._validate_data_types({column: data_type})
            base_type = data_type

        self.add_column_metadata(column, KBCMetadataKeys.base_data_type.value, base_type)
        self.add_column_metadata(column, KBCMetadataKeys.data_type_nullable.value, nullable)
        if length:
            self.add_column_metadata(column, KBCMetadataKeys.data_type_length.value, length)
        if default:
            self.add_column_metadata(column, KBCMetadataKeys.data_type_default.value, default)

    def add_table_description(self, description: str):
        """
        Adds/Updates table description that is displayed in the Storage UI
        Args:
            description: str
        """
        self.add_table_metadata(KBCMetadataKeys.description.value, description)

    def add_table_metadata(self, key: str, value: str):
        """
                Add/Updates table metadata and ensures the Key is unique.
                Args:

        """
        self.table_metadata = {**self.table_metadata, **{key: value}}

    def add_column_metadata(self, column: str, key: str, value: Union[str, bool, int]):
        """
        Add/Updates column metadata and ensures the Key is unique.
        Args:

        """
        if not self.column_metadata.get(column):
            self.column_metadata[column] = dict()

        self.column_metadata[column][key] = value

    def add_multiple_column_metadata(self, column_metadata: Dict[str, List[dict]]):
        """
        Add key-value pairs to column metadata.

        **NOTE:** Ensures uniqueness
        Args:
            column_metadata: dict {"column_name":[{"some_key":"some_value"}]}
        """
        for column, metadata_list in column_metadata:
            for metadata in metadata_list:
                key = metadata.items()[0]
                value = metadata[key]
                self.add_column_metadata(column, key, value)

    @staticmethod
    def _validate_data_types(column_types: dict):
        errors = []
        for col in column_types:
            dtype = column_types[col]
            if not SupportedDataTypes.is_valid_type(dtype):
                errors.append(f'Datatype "{dtype}" is not valid KBC Basetype!')

        raise ValueError(', '.join(errors) + f'\n Supported base types are: [{SupportedDataTypes.list()}]')


class TableDefinition:
    """
    Table definition class. It is used as a container for `in/tables/` files.

    Also, it is useful when collecting results and building export configs.

    Attributes:
        name: Table / file name.
        full_path (str): (optional) Full path of the file. May be empty in case it represents only orphaned manifest.
            May also be a folder path - in this case it is a [sliced tables](
            https://developers.keboola.com/extend/common-interface/folders/#sliced-tables) folder.
            The full_path is None when dealing with [workspaces](
            https://developers.keboola.com/extend/common-interface/folders/#exchanging-data-via-workspace)
        is_sliced: True if the full_path points to a folder with sliced tables
        destination: String name of the table in Storage.
        primary_key: List with names of columns used for primary key.
        columns: List of columns for headless CSV files
        incremental: Set to true to enable incremental loading
        table_metadata: <.dao.TableMetadata> object containing column and table metadata
        delete_where: Dict with settings for deleting rows
    """

    def __init__(self, name: str, full_path: Union[str, None] = None, is_sliced: bool = False,
                 destination: str = '',
                 primary_key: List[str] = None,
                 columns: List[str] = None,
                 incremental: bool = None,
                 table_metadata: TableMetadata = None,
                 delete_where: str = None):
        """

        Args:
            name: Table / file name.
            full_path (str):
                (optional) Full path of the file. May be empty in case it represents only orphaned
                manifest.
                May also be a folder path - in this case it is a [sliced tables](
                https://developers.keboola.com/extend/common-interface/folders/#sliced-tables) folder.
                The full_path is None when dealing with [workspaces](
                https://developers.keboola.com/extend/common-interface/folders/#exchanging-data-via-workspace)
            is_sliced: True if the full_path points to a folder with sliced tables
            destination: String name of the table in Storage.
            primary_key: List with names of columns used for primary key.
            columns: List of columns for headless CSV files
            incremental: Set to true to enable incremental loading
            table_metadata: <.dao.TableMetadata> object containing column and table metadata
            delete_where: Dict with settings for deleting rows
        """
        self.full_path = full_path
        self.name = name
        self.is_sliced = is_sliced
        self._raw_manifest = dict()

        # initialize manifest properties
        self.destination = destination
        self.primary_key = primary_key
        self.columns = columns
        self.incremental = incremental
        self.table_metadata = table_metadata
        self.set_delete_where_from_dict(delete_where)

    @classmethod
    def build_from_manifest(cls, name: str,
                            full_path: Union[str, None] = None,
                            is_sliced: bool = False,
                            raw_manifest_json: dict = None):
        """
        Factory method for TableDefinition from the raw "manifest".

        Args:
            name: Table / file name.
            full_path (str):
                (optional) Full path of the file. May be empty in case it represents only orphaned
                manifest.
                May also be a folder path - in this case it is a [sliced tables](
                https://developers.keboola.com/extend/common-interface/folders/#sliced-tables) folder.
                The full_path is None when dealing with [workspaces](
                https://developers.keboola.com/extend/common-interface/folders/#exchanging-data-via-workspace)
            is_sliced: True if the full_path points to a folder with sliced tables
            raw_manifest_json (dict): raw manifest dict as provided by Keboola Connection (data folder)
        """
        table_def = cls(name=name, full_path=full_path,
                        is_sliced=is_sliced)
        # build manifest definition
        table_def._raw_manifest = raw_manifest_json
        return table_def

    # #### Manifest properties
    @property
    def destination(self) -> str:
        return self._raw_manifest.get('destination', '')

    @destination.setter
    def destination(self, val: str):
        if val:
            if isinstance(val, str):
                self._raw_manifest['destination'] = val
            else:
                raise TypeError("Destination must be a string")

    @property
    def columns(self) -> List[str]:
        return self._raw_manifest.get('columns', [])

    @columns.setter
    def columns(self, val: List[str]):
        if val:
            if isinstance(val, list):
                self._raw_manifest['columns'] = val
            else:
                raise TypeError("Columns must by a list")

    @property
    def incremental(self) -> bool:
        return self._raw_manifest.get('incremental', False)

    @incremental.setter
    def incremental(self, incremental: bool):
        if incremental:
            self._raw_manifest['incremental'] = True

    @property
    def primary_key(self) -> List[str]:
        return self._raw_manifest.get('primary_key', [])

    @primary_key.setter
    def primary_key(self, primary_key: List[str]):
        if primary_key:
            if isinstance(primary_key, list):
                self._raw_manifest['primary_key'] = primary_key
            else:
                raise TypeError("Primary key must be a list")

    @property
    def delimiter(self) -> str:
        return self._raw_manifest.get('delimiter', ',')

    @delimiter.setter
    def delimiter(self, delimiter):
        self._raw_manifest['delimiter'] = delimiter

    @property
    def enclosure(self) -> str:
        return self._raw_manifest.get('enclosure', '"')

    @enclosure.setter
    def enclosure(self, enclosure):
        self._raw_manifest['enclosure'] = enclosure

    @property
    def table_metadata(self) -> TableMetadata:
        return self._table_metadata

    @table_metadata.setter
    def table_metadata(self, table_metadata: TableMetadata):
        self._table_metadata = table_metadata
        self._set_table_metadata_to_manifest(table_metadata)

    def set_delete_where_from_dict(self, delete_where):
        """
        Process metadata as dictionary and returns modified manifest

        Args:
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
                self._raw_manifest['delete_where_values'] = delete_where['values']
                self._raw_manifest['delete_where_column'] = delete_where['column']
                self._raw_manifest['delete_where_operator'] = op
            else:
                raise ValueError("Delete where specification must contain "
                                 "keys 'column' and 'values'")

    def _set_table_metadata_to_manifest(self, table_metadata: TableMetadata):
        self._raw_manifest['metadata'] = table_metadata.get_table_metadata_for_manifest()
        self._raw_manifest['column_metadata'] = table_metadata.get_column_metadata_for_manifest()

    def get_manifest_dictionary(self) -> dict:
        # in case the table_metadata is out of sync, e.g. the object was modified in-place
        self._set_table_metadata_to_manifest(self._table_metadata)
        return self._raw_manifest


# ####### CONFIGURATION
@dataclass
class TableColumnTypes(SubscriptableDataclass):
    """
    Abstraction of [column types](https://developers.keboola.com/extend/common-interface/config-file/#input-mapping
    --column-types) in the config file.
    Applicable only for workspace.
    """

    source: str
    type: str
    destination: str
    length: int
    nullable: bool
    convert_empty_values_to_null: bool


@dataclass
class TableInputMapping(SubscriptableDataclass):
    """
    Abstraction of [input mapping definition](
    https://developers.keboola.com/extend/common-interface/config-file/#tables) in the config file
    """
    source: str
    destination: str
    limit: int
    columns: List[str]
    where_values: List[str]
    full_path: str = None
    where_operator: str = ''
    days: int = 0
    column_types: TableColumnTypes = None


@dataclass
class TableOutputMapping(SubscriptableDataclass):
    """
    Abstraction of [output mapping definition](
    https://developers.keboola.com/extend/common-interface/config-file/#tables) in the config file
    """
    source: str
    destination: str
    incremental: bool = False
    columns: str = ''
    primary_key: str = ''
    delete_where_column: str = ''
    delete_where_operator: str = ''
    delete_where_values: str = ''
    delimiter: str = ''
    enclosure: str = ''


@dataclass
class FileInputMapping(SubscriptableDataclass):
    """
    Abstraction of [output mapping definition](
    https://developers.keboola.com/extend/common-interface/config-file/#files) in the config file
    """
    tags: List[str]
    query: str = ''
    filter_by_run_id: bool = False


@dataclass
class FileOutputMapping(SubscriptableDataclass):
    """
    Abstraction of [output mapping definition](
    https://developers.keboola.com/extend/common-interface/config-file/#files) in the config file
    """
    source: str
    is_public: bool = False
    is_permanent: bool = False
    tags: List[str] = dataclasses.field(default_factory=lambda: [])


@dataclass
class OauthCredentials(SubscriptableDataclass):
    id: str
    created: str
    data: str
    oauthVersion: str
    appKey: str
    appSecret: str


def build_dataclass_from_dict(data_class, dict_value):
    """
    Convenience method building specified dataclass from a dictionary

    Args:
        data_class:
        dict_value:

    Returns: dataclass of specified type

    """
    field_names = set(f.name for f in dataclasses.fields(data_class))
    return data_class(**{k: v for k, v in dict_value.items() if k in field_names})
