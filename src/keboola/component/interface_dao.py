import dataclasses
from dataclasses import dataclass, field
from enum import Enum

from typing import List


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


class ColumnDataTypes(Enum):
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
    description = 'KBC.description'
    created_by_component = 'KBC.createdBy.component.id'
    last_updated_by_component = 'KBC.lastUpdatedBy.component.id'


class TableMetadata:
    def __init__(self):
        self._table_metadata_dict = dict()
        self._column_metadata_dict = dict()
        self._custom_table_metadata_nonunique = list()
        self._custom_column_metadata_nonunique = dict()

    @property
    def table_metadata(self) -> List[dict]:
        """
        Returns table metadata list as required by the
        [manifest format](https://developers.keboola.com/extend/common-interface/manifest-files/#dataintables-manifests)

        Returns: List[dict]

        """
        final_metadata_list = list()
        final_metadata_list.extend(self._custom_table_metadata_nonunique)
        for key in self._table_metadata_dict:
            final_metadata_list.append({key: self._table_metadata_dict[key]})

        return final_metadata_list

    @property
    def column_metadata(self) -> dict:
        """
                Returns column metadata dict as required by the
                [manifest format](https://developers.keboola.com/extend/common-interface/manifest-files/#dataintables
                -manifests)

                Returns: dict

        """
        final_column_metadata = self._custom_column_metadata_nonunique

        # collect unique metadata keys
        for column in self._column_metadata_dict:
            column_metadata_dicts = self._column_metadata_dict[column]
            if not final_column_metadata.get(column):
                final_column_metadata[column] = list()

            column_metadata = [{key: column_metadata_dicts[key]} for key in
                               column_metadata_dicts[column]]
            final_column_metadata[column].extend(column_metadata)

        # collect non_unique metadata keys
        for column in self._custom_column_metadata_nonunique:
            if not final_column_metadata.get(column):
                final_column_metadata[column] = list()

            final_column_metadata[column].extend(self._custom_column_metadata_nonunique[column])

        return final_column_metadata

    @property
    def table_description(self) -> str:
        """
        Returns table description (KBC.description)

        Returns: str

        """
        return self._table_metadata_dict.get(KBCMetadataKeys.description.value)

    @property
    def column_datatypes(self) -> dict:
        """
        Return dictionary of column base datatypes
        e.g. {"col1name":"basetype"}

        Returns: dict e.g. {"col1name":"basetype"}

        """

        return self._get_unique_column_metadata_by_key(KBCMetadataKeys.base_data_type.value)

    @property
    def column_descriptions(self) -> dict:
        """
        Return dictionary of column descriptions
        e.g. {"col1name":"desc"}

        Returns: dict e.g. {"col1name":"desc"}

        """

        return self._get_unique_column_metadata_by_key(KBCMetadataKeys.description.value)

    def _get_unique_column_metadata_by_key(self, metadata_key):
        """
        Return dictionary of column:metadata_key pairs
        e.g. {"col1name":"value_of_metadata_with_the_key"}

        Returns: dict e.g. {"col1name":"value_of_metadata_with_the_key"}

        """
        column_types = dict()
        # we know its in unique
        for col in self._column_metadata_dict:
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
            self.add_unique_column_metadata(col, KBCMetadataKeys.description.value, column_descriptions[col])

    def add_column_types(self, column_types: dict):
        """
        Add column types metadata. Note that only supported datatypes (<keboola.component.interface.KBCMetadataKeys>)
        may be provided

        Args:
            column_types: dict -> {"colname":"datatype"}

        """
        self._validate_data_types(column_types)

        for col in column_types:
            self.add_unique_column_metadata(col, KBCMetadataKeys.base_data_type.value, column_types[col])

    def add_table_description(self, description: str):
        """
        Adds/Updates table description that is displayed in the Storage UI
        Args:
            description: str
        """
        self.add_custom_table_metadata({KBCMetadataKeys.description.value: description})

    def add_custom_table_metadata(self, key: str, value: str):
        """
        Add custom key-value pair to table metadata. NOTE: does not ensure the keys unique metadata keys.
        Args:
            key: str "some_key"
            value: str "some_value"

        """
        # process unique types
        if key == KBCMetadataKeys.description.value:
            self.add_unique_table_metadata(key, value)

        else:
            self._custom_table_metadata_nonunique.append({key: value})

    def add_unique_table_metadata(self, key: str, value: str):
        """
                Add/Updates table metadata and ensures the Key is unique.
                Args:

        """
        self._table_metadata_dict = {**self._table_metadata_dict, **{key: value}}

    def add_unique_column_metadata(self, column: str, key: str, value: str):
        """
        Add/Updates column metadata and ensures the Key is unique.
        Args:

        """
        if not self._column_metadata_dict.get(column):
            self._column_metadata_dict[column] = dict()

        self._column_metadata_dict[column][key] = value

    def add_custom_column_metadata(self, column: str, key: str, value: str):
        """
        Add custom key-value pair to column metadata.
        NOTE: Ensures uniqueness of basetype and description keys
        Args:
            column: column name
            key: str "some_key"
            value: str "some_value"

        """
        if not self._custom_column_metadata_nonunique.get(column):
            self._custom_column_metadata_nonunique[column] = list()

        # ensure uniqueness of unique values
        if key in [KBCMetadataKeys.base_data_type.value, KBCMetadataKeys.description.value]:
            self.add_unique_column_metadata(column, key, value)

        self._custom_column_metadata_nonunique[column].append({key: value})

    def add_multiple_custom_table_metadata(self, table_metadata: list):
        """
        Add custom key-value pairs to table metadata. NOTE: does not ensure the keys unique metadata keys.
        Args:
            table_metadata: list [{"some_key":"some_value"}]
        """
        self._custom_table_metadata_nonunique.extend(table_metadata)

    def add_multiple_custom_column_metadata(self, column_metadata: dict[str:List[dict]]):
        """
        Add custom key-value pairs to column metadata.
        NOTE: Ensures uniqueness of basetype and description keys
        Args:
            column_metadata: dict {"column_name":[{"some_key":"some_value"}]}
        """
        for column, metadata_list in column_metadata:
            for metadata in metadata_list:
                key = metadata.items()[0]
                value = metadata[key]
                self.add_custom_column_metadata(column, key, value)

    @staticmethod
    def _validate_data_types(column_types):
        errors = []
        for col in column_types:
            dtype = column_types[col]
            if not ColumnDataTypes.is_valid_type(dtype):
                errors.append(f'Datatype "{dtype}" is not valid KBC Basetype!')

        raise ValueError(', '.join(errors) + f'\n Supported base types are: [{ColumnDataTypes.list()}]')


def get_table_metadata_from_manifest(manifest: dict) -> TableMetadata:
    """
    Helper method for retrieving TableMetadata object from manifest file.

    Args:
        manifest:

    Returns:TableMetadata

    """
    existing_metadata = TableMetadata()
    existing_metadata.add_multiple_custom_column_metadata(manifest.get('column_metadata'))
    existing_metadata.add_multiple_custom_table_metadata(manifest.get('metadata'))
    return existing_metadata


class TableDef:
    """
    Table definition class.

    """

    def __init__(self, full_path: str, file_name: str, is_sliced: bool = False,
                 manifest: dict = field(default_factory=dict)):
        self.full_path = full_path
        self.file_name = file_name
        self.is_sliced = is_sliced
        self.manifest = manifest

    def replace_table_metadata_in_manifest(self, table_metadata: TableMetadata):
        """
        Replace the TableMetadata (metadata, column_metadata) in the manifest file.
        To get the current TableMetadata object call get_table_metadata() function

        Args:
            table_metadata:

        Returns:

        """

        self.manifest['metadata'] = table_metadata.table_metadata
        self.manifest['column_metadata'] = table_metadata.column_metadata

    def get_table_metadata(self) -> TableMetadata:
        """
        Returns copy of the TableMetadata object from the loaded manifest.

        NOTE: Write the modified object back to the manifest call the :meth: <replace_table_metadata_in_manifest()>
        method
        Returns:

        """
        return get_table_metadata_from_manifest(self.manifest)


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
