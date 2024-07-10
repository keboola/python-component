import dataclasses
import json
import logging
import warnings
from abc import ABC, abstractmethod
from dataclasses import dataclass
from datetime import datetime
from enum import Enum
from pathlib import Path
from typing import List, Union, Dict, Optional
from .exceptions import UserException

from deprecated import deprecated


try:
    from typing import Literal
except ImportError:
    from typing_extensions import Literal

KBC_DEFAULT_TIME_FORMAT = '%Y-%m-%dT%H:%M:%S%z'


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
    config_row_id: str
    branch_id: str
    staging_file_provider: str

    project_name: str
    token_id: str
    token_desc: str
    token: str
    url: str
    real_user: str

    logger_addr: str
    logger_port: str

    data_type_support: str


class SupportedDataTypes(str, Enum):
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
    base_data_type = 'KBC.datatype.basetype'  # base type of a column as defined in php-datatypes
    source_data_type = 'KBC.datatype.type'  # data type of a column - extracted value from the source
    data_type_nullable = 'KBC.datatype.nullable'
    data_type_length = 'KBC.datatype.length'  # data type length (e.g., VARCHAR(255) - this is the 255
    data_type_default = 'KBC.datatype.default'
    description = 'KBC.description'
    created_by_component = 'KBC.createdBy.component.id'
    last_updated_by_component = 'KBC.lastUpdatedBy.component.id'
    createdBy_configuration_id = 'KBC.createdBy.configuration.id'
    createdBy_branch_id = 'KBC.createdBy.branch.id'  # ID of the branch whose job created the table/bucket
    lastUpdatedBy_configuration_id = 'KBC.lastUpdatedBy.configuration.id'
    lastUpdatedBy_branch_id = 'KBC.lastUpdatedBy.branch.id'  # ID of the branch whose job last touched the bucket/table
    shared_description = 'KBC.sharedDescription'  # description of the bucket;
    # it will be used when the bucket is shared


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

        if manifest.get('schema') and (manifest.get('metadata') or manifest.get('column_metadata') or manifest.get('columns')): # noqa
            raise UserException("Manifest can't contain new 'schema' and old 'metadata'/'column_metadata'/'columns'")

        if not manifest.get('schema'):

            # column metadata
            for column, metadata_list in manifest.get('column_metadata', {}).items():
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

    def get_table_metadata_for_manifest(self, new_manifest: bool = False) -> List[dict]:
        """
        Returns table metadata list as required by the
        [manifest format]
        (https://developers.keboola.com/extend/common-interface/manifest-files/#dataintables-manifests)

        e.g.
        tm = TableMetadata()
        manifest['metadata'] = tm.table_metadata

        Returns: List[dict]

        """
        if new_manifest:
            final_metadata_list = [{key: self.table_metadata[key]}
                                   for key in self.table_metadata]
        else:
            final_metadata_list = [{'key': key,
                                    'value': self.table_metadata[key]}
                                   for key in self.table_metadata]

        return final_metadata_list

    @deprecated(version='1.5.1', reason="Please use schema instead of Table Metadata")
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
                               column_metadata_dicts]
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
    @deprecated(version='1.5.1', reason="Column datatypes were moved to dao.TableDefinition.schema property."
                                        "Please use the dao.ColumnDefinition objects")
    def column_datatypes(self) -> dict:
        """
        Return dictionary of column base datatypes
        e.g. {"col1name":"basetype"}

        Returns: dict e.g. {"col1name":"basetype"}

        """

        return self.get_columns_metadata_by_key(KBCMetadataKeys.base_data_type.value)

    @property
    @deprecated(version='1.5.1', reason="Column datatypes were moved to dao.TableDefinition.schema property."
                                        " Please use the dao.ColumnDefinition objects")
    def column_descriptions(self) -> dict:
        """
        Return dictionary of column descriptions
        e.g. {"col1name":"desc"}

        Returns: dict e.g. {"col1name":"desc"}

        """

        return self.get_columns_metadata_by_key(KBCMetadataKeys.description.value)

    @deprecated(version='1.5.1', reason="Please use schema instead of Table Metadata")
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

    @deprecated(version='1.5.1', reason="Column datatypes were moved to dao.TableDefinition.schema property."
                                        "Please use the dao.ColumnDefinition objects and associated"
                                        "dao.TableDefinition methods to define columns. e.g."
                                        "dao.TableDefinition.add_columns()")
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

    @deprecated(version='1.5.1', reason="Column datatypes were moved to dao.TableDefinition.schema property."
                                        "Please use the dao.ColumnDefinition objects and associated"
                                        "dao.TableDefinition methods to define columns. e.g."
                                        "dao.TableDefinition.add_column()")
    def add_column_data_type(self, column: str, data_type: Union[SupportedDataTypes, str],
                             source_data_type: str = None,
                             nullable: bool = False,
                             length: str = None, default=None):
        """
        Add single column data type
        Args:
            column (str): name of the column
            data_type (Union[SupportedDataTypes, str]):
                Either instance of ColumnDataTypes enum or a valid string. Basetype supported by KBC.
                base type of a column as defined in
                [php-datatypes](https://github.com/keboola/php-datatypes#base-types);
                see getBaseType implementations (e.g., [mysql](https://github.com/keboola/
                php-datatypes/blob/325fe4eff3e3dfae986ebbdb769eaefd18be6086/src/Definition/MySQL.php#L225))
                for mapping between KBC.datatype.type and KBC.datatype.basetype
            source_data_type (str):
                Optional. Data type of a column - extracted value from the source.
            nullable (bool): Is column nullable? KBC input mapping converts empty values to NULL
            length (str): Column length when applicable e.g. 39,8; 4000
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

        if source_data_type is not None:
            self.add_column_metadata(column, KBCMetadataKeys.source_data_type.value, source_data_type)

        if length is not None:
            self.add_column_metadata(column, KBCMetadataKeys.data_type_length.value, length)
        if default is not None:
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

    def add_column_metadata(self, column: str, key: str, value: Union[str, bool, int], backend="base"):
        """
        Add/Updates column metadata and ensures the Key is unique.
        Args:

        """
        if not self.column_metadata.get(column):
            self.column_metadata[column] = dict()

        self.column_metadata[column][key] = value

        # self.schema = [ColumnDefinition(name=column, data_type={backend: DataType(type=value)})]

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
        if errors:
            raise ValueError(', '.join(errors) + f'\n Supported base types are: [{SupportedDataTypes.list()}]')


@dataclass
class DataType:
    type: Union[SupportedDataTypes, str]
    length: Optional[int] = None
    default: Optional[str] = None

    def __post_init__(self):
        if isinstance(self.type, SupportedDataTypes):
            self.type = self.type.value


@dataclass
class BaseType(DataType):
    pass


@dataclass
class ColumnDefinition:
    name: Optional[str] = None
    data_type: Optional[Union[Dict[str, DataType], BaseType]] = None
    nullable: Optional[bool] = True
    primary_key: Optional[bool] = False
    description: Optional[str] = None
    metadata: Optional[Dict[str, str]] = None

    def __post_init__(self):
        if self.data_type:
            self.data_type = self.normalize_data_type(self.data_type)

            base_type = self.data_type.get('base')
            if base_type and not SupportedDataTypes.is_valid_type(base_type.type):
                raise ValueError(f'Datatype "{base_type.type}" is not valid KBC Basetype!'
                                 f'\n Supported base types are: [{SupportedDataTypes.list()}]')

        else:
            self.data_type = {"base": DataType(type="STRING")}

    def normalize_data_type(self, data_type: Union[Dict[str, DataType], BaseType]) -> Dict[str, DataType]:
        if isinstance(data_type, DataType):
            return {"base": data_type}
        return data_type

    def update_properties(self, **kwargs):
        for key, value in kwargs.items():
            if hasattr(self, key):
                setattr(self, key, value)
            else:
                raise AttributeError(f"{key} is not a valid attribute of {self.__class__.__name__}")

    def from_dict(self, col: dict):
        return ColumnDefinition(
            name=col.get('name'),
            data_type={key: DataType(type=v.get('type'), default=v.get('default'), length=v.get('length'))
                       for key, v in col.get('data_type', {}).items()},
            nullable=col.get('nullable'),
            primary_key=col.get('primary_key'),
            description=col.get('description'),
            metadata=col.get('metadata'))

    def to_dict(self):
        result = {
            'name': self.name,
            'data_type': {outer_key: {inner_key: value for inner_key, value in vars(outer_value).items() if value}
                          for outer_key, outer_value in self.data_type.items()} if self.data_type else None,
            'nullable': self.nullable,
            'primary_key': self.primary_key,
            'description': self.description,
            'metadata': self.metadata
        }

        filtered = {k: v for k, v in result.items() if v not in [None, False]}

        return filtered


@dataclass
class SupportedManifestAttributes(SubscriptableDataclass):
    out_attributes: List[str]
    in_attributes: List[str]
    out_legacy_exclude: List[str] = dataclasses.field(default_factory=lambda: [])
    in_legacy_exclude: List[str] = dataclasses.field(default_factory=lambda: [])

    def get_attributes_by_stage(self, stage: Literal['in', 'out'], legacy_queue: bool = False,
                                native_types: bool = False) -> List[str]:
        if stage == 'out':
            attributes = self.out_attributes
            exclude = self.out_legacy_exclude

            if native_types:
                to_remove = ['primary_key', 'columns', 'distribution_key', 'column_metadata', 'metadata']
                attributes = list(set(attributes).difference(to_remove))

                to_add = ['manifest_type', 'has_header', 'description', 'table_metadata', 'schema']
                attributes.extend(to_add)

        elif stage == 'in':
            attributes = self.in_attributes
            exclude = self.in_legacy_exclude
        else:
            raise ValueError(f'Unsupported stage {stage}')

        if legacy_queue:
            logging.warning(f'Running on legacy queue some manifest properties will be ignored: {exclude}')
            attributes = list(set(attributes).difference(exclude))

        return attributes


class IODefinition(ABC):

    def __init__(self, full_path):
        self.full_path = full_path

        # infer stage by default
        self.__stage = self.__get_stage_inferred()

    @classmethod
    def build_from_manifest(cls,
                            manifest_file_path: str
                            ):
        raise NotImplementedError

    def _filter_attributes_by_manifest_type(self, manifest_type: Literal["in", "out"], legacy_queue: bool = False,
                                            native_types: bool = False):
        """
        Filter manifest to contain only supported fields
        Args:
            manifest_type:

        Returns:

        """

        if isinstance(self, TableDefinition):
            supported_fields = self._manifest_attributes.get_attributes_by_stage(manifest_type, legacy_queue,
                                                                                 native_types)
            fields = {
                'id': self.id,
                'uri': self._uri,
                'name': self.name,
                'created': self._created,
                'last_change_date': self._last_change_date,
                'last_import_date': self._last_import_date,
                'rows_count': self._rows_count,
                'data_size_bytes': self._data_size_bytes,
                'is_alias': self._is_alias,

                'destination': self.destination,
                'columns': self.columns if native_types else self.legacy_columns,
                'incremental': self.incremental,
                'primary_key': self.primary_key,
                'write_always': self.write_always,
                'delimiter': self.delimiter,
                'enclosure': self.enclosure,
                'metadata': self.table_metadata.get_table_metadata_for_manifest(),
                'column_metadata': self.table_metadata.get_column_metadata_for_manifest(),
                'manifest_type': manifest_type,
                'has_header': self.has_header,
                'description': None,
                'table_metadata': self.table_metadata.get_table_metadata_for_manifest(new_manifest=True),
                'delete_where_column': self.delete_where_column,
                'delete_where_values': self.delete_where_values,
                'delete_where_operator': self.delete_where_operator,
                'schema': [col.to_dict() for col in self.schema] if self.schema else []
            }

            new_dict = fields.copy()

            if supported_fields:
                for attr in fields:
                    if attr not in supported_fields:
                        new_dict.pop(attr, None)
            return new_dict

        else:
            return {
                'id': self.id,
                'created': self.created.strftime('%Y-%m-%dT%H:%M:%S%z') if self.created else None,
                'is_public': self.is_public,
                'is_encrypted': self.is_encrypted,
                'name': self.name,
                'size_bytes': self.size_bytes,
                'tags': self.tags,
                'notify': self.notify,
                'max_age_days': self.max_age_days,
                'is_permanent': self.is_permanent,
            }

    def _has_header_in_file(self):
        if self.is_sliced:
            has_header = False
        elif self.columns and not self.stage == 'in':
            has_header = False
        else:
            has_header = True
        return has_header

    def get_manifest_dictionary(self, manifest_type: Optional[str] = None, legacy_queue: bool = False,
                                native_types: bool = False) -> dict:
        """
        Returns manifest dictionary in appropriate manifest_type: either 'in' or 'out'.
        By default, returns output manifest.
             The result keeps only values that are applicable for
             the selected type of the Manifest file. Because although input and output manifests share most of
             the attributes, some are not shared.

             See [manifest files](https://developers.keboola.com/extend/common-interface/manifest-files)
             for more information.

        Args:
            manifest_type (str): either 'in' or 'out'.
             See [manifest files](https://developers.keboola.com/extend/common-interface/manifest-files)
             for more information.
            legacy_queue (bool): optional flag marking project on legacy queue.(some options are not allowed on queue2)
            native_types (bool): optional flag marking if the manifest should be new, default to False - legacy format

        Returns:
            dict representation of the manifest file in a format expected / produced by the Keboola Connection

        """
        if not manifest_type:
            manifest_type = self.stage

        dictionary = self._filter_attributes_by_manifest_type(manifest_type, legacy_queue, native_types)

        filtered_dictionary = {k: v for k, v in dictionary.items() if v is not None and v != [] and v != ""}

        return filtered_dictionary

    @property
    def stage(self) -> str:
        """
        Helper property marking the stage of the file. (str)
        """
        return self.__stage

    @stage.setter
    def stage(self, stage: str):
        if stage not in ['in', 'out']:
            raise ValueError(f'Invalid stage "{stage}", supported values are: "in", "out"')
        self.__stage = stage

    @property
    @abstractmethod
    def _manifest_attributes(self) -> SupportedManifestAttributes:
        """
        Manifest attributes
        """
        return SupportedManifestAttributes([], [])

    @property
    @abstractmethod
    def name(self) -> str:
        """
        File name - excluding the KBC ID if present (`str`, read-only)
        """
        raise NotImplementedError

    def __get_stage_inferred(self):
        stage = 'in'
        if not self.full_path or not Path(self.full_path).exists():
            return stage

        if Path(self.full_path).parent.parent.name == 'in':
            stage = 'in'
        elif Path(self.full_path).parent.parent.name == 'out':
            stage = 'out'
        return stage

    # ############ Staging parameters

    @dataclass
    class S3Staging:
        is_sliced: bool
        region: str
        bucket: str
        key: str
        credentials_access_key_id: str
        credentials_secret_access_key: str
        credentials_session_token: str

    @dataclass
    class ABSStaging:
        is_sliced: bool
        region: str
        container: str
        name: str
        credentials_sas_connection_string: str
        credentials_expiration: str

    @property
    def s3_staging(self) -> Union[S3Staging, None]:
        s3 = self._s3
        if s3:
            return IODefinition.S3Staging(is_sliced=s3['isSliced'],
                                          region=s3['region'],
                                          bucket=s3['bucket'],
                                          key=s3['key'],
                                          credentials_access_key_id=s3['credentials']['access_key_id'],
                                          credentials_secret_access_key=s3['credentials']['secret_access_key'],
                                          credentials_session_token=s3['credentials']['session_token']
                                          )
        else:
            return None

    @property
    def abs_staging(self) -> Union[ABSStaging, None]:
        _abs = self._abs
        if _abs:
            return IODefinition.ABSStaging(is_sliced=_abs['is_sliced'],
                                           region=_abs['region'],
                                           container=_abs['container'],
                                           name=_abs['name'],
                                           credentials_sas_connection_string=_abs['credentials'][
                                               'sas_connection_string'],
                                           credentials_expiration=_abs['credentials']['expiration']
                                           )
        else:
            return None


class TableDefinition(IODefinition):
    """
    Table definition class. It is used as a container for `in/tables/` files.
    It is a representation of input/output manifest objects with additional attributes containing information
    about related file full path and whether it is a sliced table.

    Also, it is useful when collecting results and building export configs.

    To create the TableDefinition directly from the manifest there is a factory build method:

    ```python
    from keboola.component import CommonInterface
    from keboola.component import dao

    table_def = dao.TableDefinition.build_from_manifest(manifest_dict,
                                            'table name',
                                            full_path='optional full path',
                                            is_sliced=False)


    ```


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

    INPUT_MANIFEST_ATTRIBUTES = [
        "id",
        "uri",
        "name",
        "primary_key",
        "created",
        "last_change_date",
        "last_import_date",
        "columns",
        "metadata",
        "column_metadata",
    ]

    OUTPUT_MANIFEST_ATTRIBUTES = [
        "destination",
        "columns",
        "incremental",
        "primary_key",
        "write_always",
        "delimiter",
        "enclosure",
        "metadata",
        "column_metadata",
        "delete_where_column",
        "delete_where_values",
        "delete_where_operator",
    ]

    OUTPUT_MANIFEST_LEGACY_EXCLUDES = [
        "write_always"
    ]

    MANIFEST_ATTRIBUTES = {'in': INPUT_MANIFEST_ATTRIBUTES,
                           'out': OUTPUT_MANIFEST_ATTRIBUTES}

    def __init__(self, name: str,
                 full_path: Union[str, None] = None,
                 is_sliced: bool = False,

                 destination: str = '',
                 primary_key: List[str] = None,
                 columns: List[str] = None,
                 incremental: bool = None,
                 table_metadata: TableMetadata = None,
                 enclosure: str = '"',
                 delimiter: str = ',',
                 delete_where: dict = None,
                 stage: str = 'in',
                 write_always: bool = False,
                 schema: List[ColumnDefinition] = None,
                 rows_count: int = None,
                 data_size_bytes: int = None,
                 is_alias: bool = False,
                 has_header: bool = None,

                 # input
                 uri: str = None,
                 id: str = '',
                 created: str = None,
                 last_change_date: str = None,
                 last_import_date: str = None,
                 ):
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
            table_metadata: <.dao.TableMetadata> object containing column and table metadata (deprecated)
            enclosure: str: CSV enclosure, by default "
            delimiter: str: CSV delimiter, by default ,
            delete_where (dict): Dict with settings for deleting rows
            stage: str: Storage Stage 'in' or 'out'
            write_always: Bool: If true, the table will be saved to Storage even when the job execution
                           fails.
            schema: List of ColumnDefinition objects

        """
        super().__init__(full_path)
        self._name = name
        self.is_sliced = is_sliced

        self.schema = schema

        # initialize manifest properties
        self._destination = None
        self.destination = destination
        self.columns = columns
        self.primary_key = primary_key
        self._incremental = incremental

        self.enclosure = enclosure
        self.delimiter = delimiter

        if not table_metadata:
            table_metadata = TableMetadata()
        self.table_metadata = table_metadata

        self.delete_where_values = None
        self.delete_where_column = None
        self.delete_where_operator = None

        self.set_delete_where_from_dict(delete_where)
        self.stage = stage
        self.write_always = write_always
        self.legacy_columns = columns
        self._rows_count = rows_count
        self._data_size_bytes = data_size_bytes
        self._is_alias = is_alias
        self.has_header = has_header or self._has_header_in_file()

        # input manifest properties
        self._id = id
        self._uri = uri
        self._created = created
        self._last_change_date = last_change_date
        self._last_import_date = last_import_date

    @classmethod
    def build_output_definition(cls, name: str,
                                destination: str,
                                columns: List[str],
                                primary_key: List[str] = None,
                                incremental: bool = False,
                                table_metadata: TableMetadata = None,
                                enclosure: str = '"',
                                delimiter: str = ',',
                                delete_where: dict = None,
                                write_always: bool = False,
                                schema: List[ColumnDefinition] = None,
                                ):
        """
        Factory method for TableDefinition for output tables.
        Args:
            name: Table / file name.
            destination: String name of the table in Storage.
            columns: List of columns for headless CSV files
            primary_key: List with names of columns used for primary key.
            incremental: Set to true to enable incremental loading
            table_metadata: <.dao.TableMetadata> object containing column and table metadata (deprecated)
            enclosure: str: CSV enclosure, by default "
            delimiter: str: CSV delimiter, by default ,
            delete_where: Dict with settings for deleting rows
            write_always: Bool: If true, the table will be saved to Storage even when the job execution
                           fails.
            schema: List of ColumnDefinition objects

        Returns: TableDefinition
        """
        return cls(name=name,
                   destination=destination,
                   columns=columns,
                   primary_key=primary_key,
                   incremental=incremental,
                   table_metadata=table_metadata,
                   enclosure=enclosure,
                   delimiter=delimiter,
                   delete_where=delete_where,
                   write_always=write_always,
                   schema=schema,
                   )

    @classmethod
    def build_input_definition(cls, name: str,
                               full_path: Union[str, None] = None,
                               is_sliced: bool = False,

                               destination: str = '',
                               primary_key: List[str] = None,
                               columns: List[str] = None,
                               incremental: bool = None,
                               table_metadata: TableMetadata = None,
                               enclosure: str = '"',
                               delimiter: str = ',',
                               delete_where: dict = None,
                               stage: str = 'in',
                               write_always: bool = False,
                               schema: List[ColumnDefinition] = None,
                               rows_count: int = None,
                               data_size_bytes: int = None,
                               is_alias: bool = False,

                               # input
                               uri: str = None,
                               id: str = '',
                               created: str = None,
                               last_change_date: str = None,
                               last_import_date: str = None):
        """
        Factory method for TableDefinition for input tables.
        """
        return cls(name=name,
                   full_path=full_path,
                   is_sliced=is_sliced,
                   destination=destination,
                   primary_key=primary_key,
                   columns=columns,
                   incremental=incremental,
                   table_metadata=table_metadata,
                   enclosure=enclosure,
                   delimiter=delimiter,
                   delete_where=delete_where,
                   stage=stage,
                   write_always=write_always,
                   schema=schema,
                   rows_count=rows_count,
                   data_size_bytes=data_size_bytes,
                   is_alias=is_alias,
                   uri=uri,
                   id=id,
                   created=created,
                   last_change_date=last_change_date,
                   last_import_date=last_import_date)

    @classmethod
    def convert_to_column_definition(cls, column_name, column_metadata, primary_key=False):
        data_type = {'base': DataType(type='STRING')}
        nullable = True
        for item in column_metadata:
            if item['key'] == 'KBC.datatype.basetype':
                data_type = {'base': DataType(type=item['value'])}
            elif item['key'] == 'KBC.datatype.nullable':
                nullable = item['value']
        return ColumnDefinition(name=column_name, data_type=data_type, nullable=nullable, primary_key=primary_key)

    @classmethod
    def return_schema_from_manifest(cls, json_data):
        if TableDefinition.is_new_manifest(json_data):
            schema = []
            for col in json_data.get('schema'):
                schema.append(ColumnDefinition().from_dict(col))

        else:
            columns_metadata = json_data.get('column_metadata', {})
            primary_key = json_data.get('primary_key', [])
            columns = json_data.get('columns', [])

            all_columns = columns
            schema = []

            for col in all_columns:
                pk = col in primary_key
                if col in columns_metadata:
                    schema.append(cls.convert_to_column_definition(col, columns_metadata[col], primary_key=pk))
                else:
                    schema.append(ColumnDefinition(name=col, data_type={"base": DataType(type="STRING")},
                                                   primary_key=pk))

        return schema

    @classmethod
    def is_new_manifest(cls, json_data):
        return json_data.get('schema')

    @classmethod
    def build_from_manifest(cls,
                            manifest_file_path: str
                            ):
        """
        Factory method for TableDefinition from the raw "manifest" path.

        The TableDefinition then validates presence of the manifest counterpart.
        E.g. table.csv if `table.csv.manifest` is provided.

        The manifest file does not need to exist, in such case a ValueError is raised
        if the counterpart table is not found.

        The counterpart table file does not need to exist, in such case, the manifest represents an orphaned manifest.

        Args:
            manifest_file_path (str):
                (optional) Full path of the manifest file. May be empty in case it represents only expected
                 table with no input manifest.


        """
        is_sliced = False
        full_path = None
        manifest = dict()
        if Path(manifest_file_path).exists():
            with open(manifest_file_path) as in_file:
                manifest = json.load(in_file)

        file_path = Path(manifest_file_path.replace('.manifest', ''))

        if file_path.is_dir() and manifest:
            is_sliced = True
        elif file_path.is_dir() and not manifest:
            # skip folders that do not have matching manifest
            raise ValueError(f'The manifest {manifest_file_path} does not exist '
                             f'and it'f's matching file {file_path} is folder!')
        elif not file_path.exists() and not manifest:
            raise ValueError(f'Nor the manifest file or the corresponding file {file_path} exist!')

        if file_path.exists():
            full_path = str(file_path)
            name = file_path.name
        else:
            name = Path(manifest_file_path).stem

        table_def = cls(name=name,
                        full_path=full_path,
                        is_sliced=is_sliced,
                        id=manifest.get('id'),
                        table_metadata=TableMetadata(manifest),
                        schema=cls.return_schema_from_manifest(manifest),
                        uri=manifest.get('uri'),
                        created=manifest.get('created'),
                        last_change_date=manifest.get('last_change_date'),
                        last_import_date=manifest.get('last_import_date'),
                        rows_count=manifest.get('rows_count'),
                        data_size_bytes=manifest.get('data_size_bytes'),
                        is_alias=manifest.get('is_alias')
                        )

        return table_def

    @property
    def schema(self):
        return self._schema

    @schema.setter
    def schema(self, value):
        self._schema = []
        if value:
            if any(not isinstance(v, ColumnDefinition) for v in value):
                raise ValueError("Schema must be an instance of ColumnDefinition")
            self._schema = value

    @property
    def _manifest_attributes(self) -> SupportedManifestAttributes:
        return SupportedManifestAttributes(self.MANIFEST_ATTRIBUTES['out'], self.MANIFEST_ATTRIBUTES['in'],
                                           self.OUTPUT_MANIFEST_LEGACY_EXCLUDES)

    # #### Manifest properties
    @property
    def destination(self) -> str:
        return self._destination

    @destination.setter
    def destination(self, val: str):
        if val:
            if isinstance(val, str):
                self._destination = val
            else:
                raise TypeError("Destination must be a string")

    @property
    def id(self) -> str:
        """
        str: id property used in input manifest. Contains Keboola Storage ID, e.g. in.c-bucket.table

        """
        return self._id

    @property
    def name(self) -> str:
        """
        File name - excluding the KBC ID if present (`str`, read-only)
        """
        return self._name

    @property
    def rows_count(self) -> int:
        """
                int: rows_count property used in input manifest.

        """
        return self._rows_count

    @property
    def data_size_bytes(self) -> int:
        """
                int: data_size_bytes property used in input manifest.

        """
        return self._data_size_bytes

    @property
    @deprecated(version='1.5.1', reason="Please use new column_names method instead of columns property")
    def columns(self) -> List[str]:
        if self.schema:
            return [col.name for col in self.schema]
        else:
            return []

    @columns.setter
    @deprecated(version='1.5.1', reason="Columns can be set by add_columns method")
    def columns(self, val: List[str]):
        if val:
            if len(self.schema) > 0:
                warnings.warn("Columns are already set. Use 'add_columns' to add new columns.")
                return

            if not isinstance(val, list):
                raise TypeError("Columns must be a list")

            for col in val:
                if col not in self.columns:
                    self.schema.append(ColumnDefinition(name=col))

    @property
    def column_names(self) -> List[str]:
        if self.schema:
            return [col.name for col in self.schema]
        else:
            return []

    @property
    def incremental(self) -> bool:
        return self._incremental

    @incremental.setter
    def incremental(self, incremental: bool):
        if incremental:
            self._incremental = True

    @property
    def write_always(self) -> bool:
        return self._write_always

    @write_always.setter
    def write_always(self, write_always: bool):
        self._write_always = write_always

    @property
    def primary_key(self) -> List[str]:
        if self.schema:
            return [col.name for col in self.schema if col.primary_key]

    @primary_key.setter
    def primary_key(self, primary_key: List[str]):
        if not primary_key:
            return

        if not isinstance(primary_key, list):
            raise TypeError("Primary key must be a list")

        for col in primary_key:
            if col not in self.columns:
                raise UserException(f"Primary key column {col} not found in columns")

            for c in self.schema:
                if c.name == col:
                    c.primary_key = True

    @property
    def delimiter(self) -> str:
        return self._delimiter

    @delimiter.setter
    def delimiter(self, delimiter: str):
        self._delimiter = delimiter

    @property
    def enclosure(self) -> str:
        return self._enclosure

    @enclosure.setter
    def enclosure(self, enclosure: str):
        self._enclosure = enclosure

    @property
    def table_metadata(self) -> TableMetadata:
        return self._table_metadata

    @table_metadata.setter
    def table_metadata(self, table_metadata: TableMetadata):
        self._table_metadata = table_metadata

    @property
    def created(self) -> Union[datetime, None]:  # Created timestamp  in the KBC Storage (read only input attribute)
        if self._created:
            return datetime.strptime(self._created, KBC_DEFAULT_TIME_FORMAT)
        else:
            return None

    def add_column(self, column: Union[str, ColumnDefinition]):
        """
        Add column definition, accepts either ColumnDefinition or a string
        (in which case the base type STRING will be used).
        """
        if isinstance(column, str):
            column = ColumnDefinition(name=column)
        if not isinstance(column, ColumnDefinition):
            raise ValueError("New column must be an instance of ColumnDefinition or a string")

        if any(existing_column.name == column.name for existing_column in self._schema):
            raise ValueError(f"Column with name '{column.name}' already exists")

        self._schema.append(column)

    def update_column(self, column: ColumnDefinition):
        if not isinstance(column, ColumnDefinition):
            raise ValueError("New column must be an instance of ColumnDefinition")

        for idx, old_column in enumerate(self._schema):
            if old_column.name == column.name:
                self._schema[idx] = column
                return

        raise ValueError(f"Column with name {column.name} not found")

    def delete_column(self, column_name: Union[str, ColumnDefinition]):

        if isinstance(column_name, ColumnDefinition):
            column_name = column_name.name

        for idx, column in enumerate(self._schema):
            if column.name == column_name:
                del self._schema[idx]
                return
        raise ValueError(f"Column with name {column_name} not found")

    def add_columns(self, columns: List[Union[str, ColumnDefinition]]):
        for column in columns:
            self.add_column(column)

    def update_columns(self, columns: List[ColumnDefinition]):
        for column in columns:
            self.update_column(column)

    def delete_columns(self, columns: List[Union[str, ColumnDefinition]]):
        for column in columns:
            self.delete_column(column)

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
                self.delete_where_values = delete_where['values']
                self.delete_where_column = delete_where['column']
                self.delete_where_operator = op
            else:
                raise ValueError("Delete where specification must contain "
                                 "keys 'column' and 'values'")

    def get_manifest_dictionary(self, stage_type: Optional[str] = None, legacy_queue=False,
                                native_types: bool = True) -> dict:
        """

        Args:
             See [manifest files](https://developers.keboola.com/extend/common-interface/manifest-files)
             for more information.


        Returns:
            dict representation of the manifest file in a format expected / produced by the Keboola Connection

        """
        raw_manifest = super(TableDefinition, self).get_manifest_dictionary(stage_type, legacy_queue, native_types)
        raw_manifest = {k: v for k, v in raw_manifest.items() if v not in [None, [], {}]}

        # TODO bez toho neprochází test test_schema.py", line 49, in test_created_manifest_against_schema
        raw_manifest = {k: v for k, v in raw_manifest.items() if
                        not ((k == "incremental" and v is False) or (k == "destination" and v == ""))}

        return raw_manifest


class FileDefinition(IODefinition):
    """
    File definition class. It is used as a container for `{in/out}/files/` files.
    It is a representation of input/output [manifest objects](
    https://developers.keboola.com/extend/common-interface/manifest-files/#files).

    Also, it is useful when collecting results and building export configs.



    To create the FileDefinition directly from the manifest there is a factory build method:

    ```python
    from keboola.component import CommonInterface
    from keboola.component import dao

    table_def = dao.FileDefinition.build_from_manifest('in/files/file.jpg.manifest')


    ```


    Attributes:
        name: File name.
        full_path (str): (optional) Full path of the file.
        tags (list):
                List of tags that are assigned to this file
        is_public: When true, the file URL will be permanent and publicly accessible.
        is_permanent: Keeps a file forever. If false, the file will be deleted after default period of time (e.g.
            15 days)
        is_encrypted: If true, the file content will be encrypted in the storage.
        notify: Notifies project administrators that a file was uploaded.

    """
    SYSTEM_TAG_PREFIXES = ['componentId:',
                           'configurationId:',
                           'configurationRowId:',
                           'runId:',
                           'branchId:']

    OUTPUT_MANIFEST_KEYS = ["tags",
                            "is_public",
                            "is_permanent",
                            "is_encrypted",
                            "notify"]

    def __init__(self, full_path: str,
                 tags: List[str] = None,
                 is_public: bool = False,
                 is_permanent: bool = False,
                 is_encrypted: bool = False,
                 notify: bool = False,
                 id: str = None,
                 s3: dict = None,
                 abs: dict = None,
                 created: str = None,
                 size_bytes: int = None,
                 max_age_days: int = None
                 ):
        """

        Args:
            full_path (str): Full path of the file.
            tags (list):
                List of tags that are assigned to this file
            is_public: When true, the file URL will be permanent and publicly accessible.
            is_permanent: Keeps a file forever. If false, the file will be deleted after default period of time (e.g.
            15 days)
            is_encrypted: If true, the file content will be encrypted in the storage.
            notify: Notifies project administrators that a file was uploaded.
        """
        super().__init__(full_path)

        self.tags = tags
        self.is_public = is_public
        self.is_permanent = is_permanent
        self.is_encrypted = is_encrypted
        self.notify = notify

        # input
        self._id = id
        self._s3 = s3
        self._abs = abs
        self._created = created
        self._size_bytes = size_bytes
        self._max_age_days = max_age_days

    @classmethod
    def build_output_definition(cls, full_path: str, tags: List[str] = None, is_public: bool = False,
                                is_permanent: bool = False, is_encrypted: bool = False, notify: bool = False):
        """
        Build output file definition
        Args:
            full_path (str): Full path of the file.
            tags (list):
                List of tags that are assigned to this file
            is_public: When true, the file URL will be permanent and publicly accessible.
            is_permanent: Keeps a file forever. If false, the file will be deleted after default period of time (e.g.
            15 days)
            is_encrypted: If true, the file content will be encrypted in the storage.
            notify: Notifies project administrators that a file was uploaded.
        """
        return cls(full_path=full_path, tags=tags, is_public=is_public, is_permanent=is_permanent,
                   is_encrypted=is_encrypted, notify=notify)

    @classmethod
    def build_input_definition(cls, full_path: str, id: str = None, s3: dict = None, abs: dict = None,
                               created: str = None, size_bytes: int = None, max_age_days: int = None):
        """
        Build input file definition
        Args:
            full_path (str): Full path of the file.
            id (str): File ID in the KBC Storage
            s3 (dict): S3 staging information
            abs (dict): ABS staging information
            created (str): Created timestamp in the KBC Storage
            size_bytes (int): File size in the KBC Storage
            max_age_days (int): File max age
        """
        return cls(full_path=full_path, id=id, s3=s3, abs=abs, created=created, size_bytes=size_bytes,
                   max_age_days=max_age_days)

    @classmethod
    def build_from_manifest(cls,
                            manifest_file_path: str
                            ):
        """
        Factory method for FileDefinition from the raw "manifest" path.

        The FileDefinition then validates presence of the manifest counterpart.
        E.g. file.jpg if `file.jpg.manifest` is provided.

        If the counterpart file does not exist a ValueError is raised.


        Args:
            manifest_file_path (str):
                (optional) Full path of the file [manifest](
                https://developers.keboola.com/extend/common-interface/manifest-files/#files)


        """
        manifest = dict()
        if Path(manifest_file_path).exists():
            with open(manifest_file_path) as in_file:
                manifest = json.load(in_file)

        file_path = Path(manifest_file_path.replace('.manifest', ''))

        if not file_path.exists():
            raise ValueError(f'The corresponding file {file_path} does not exist!')

        full_path = str(file_path)

        file_def = cls(full_path=full_path,
                       tags=manifest.get('tags', []),
                       is_public=manifest.get('is_public', False),
                       is_permanent=manifest.get('is_permanent', False),
                       is_encrypted=manifest.get('is_encrypted', False),
                       id=manifest.get('id', ''),
                       s3=manifest.get('s3'),
                       abs=manifest.get('abs'),
                       created=manifest.get('created'),
                       size_bytes=manifest.get('size_bytes', 0),
                       max_age_days=manifest.get('max_age_days', 0)
                       )

        return file_def

    @classmethod
    def is_system_tag(cls, tag: str) -> bool:
        for prefix in cls.SYSTEM_TAG_PREFIXES:
            if tag.startswith(prefix):
                return True
        return False

    @property
    def name(self) -> str:
        """
        File name - excluding the KBC ID if present (`str`, read-only)
        """
        # separate id from name
        file_name = Path(self.full_path).name
        if self._id:
            fsplit = file_name.split('_', 1)
            if len(fsplit) > 1:
                self._id = fsplit[0]
                file_name = fsplit[1]
        return file_name

    @property
    def full_name(self):
        """
        File name - full file name, directly from the path. Includes the KBC generated ID. (`str`, read-only)
        """
        return Path(self.full_path).name

    @property
    def _manifest_attributes(self) -> SupportedManifestAttributes:
        return SupportedManifestAttributes(self.OUTPUT_MANIFEST_KEYS, [])

    # ########### Output manifest properties - R/W

    @property
    def user_tags(self) -> List[str]:
        """
        User defined tags excluding the system tags
        """
        # filter system tags
        tags: List[str] = [tag for tag in self._tags if not self.is_system_tag(tag)]
        return tags

    @property
    def tags(self) -> List[str]:
        """
        All tags specified on the file
        """
        return self._tags

    @tags.setter
    def tags(self, tags: List[str]):
        if tags is None:
            tags = list()
        self._tags = tags

    @property
    def is_public(self) -> bool:
        return self._is_public

    @is_public.setter
    def is_public(self, is_public: bool):
        self._is_public = is_public

    @property
    def is_permanent(self) -> bool:
        return self._is_permanent

    @is_permanent.setter
    def is_permanent(self, is_permanent: bool):
        self._is_permanent = is_permanent

    @property
    def is_encrypted(self) -> bool:
        return self._is_encrypted

    @is_encrypted.setter
    def is_encrypted(self, is_encrypted: bool):
        self._is_encrypted = is_encrypted

    @property
    def notify(self) -> bool:
        return self._notify

    @notify.setter
    def notify(self, notify: bool):
        self._notify = notify

    # ########### Input manifest properties - Read ONLY
    @property
    def id(self) -> str:  # File ID in the KBC Storage (read only input attribute)
        return self._id

    @property
    def created(self) -> Union[datetime, None]:  # Created timestamp  in the KBC Storage (read only input attribute)
        if self._created:
            return datetime.strptime(self._created, KBC_DEFAULT_TIME_FORMAT)
        else:
            return None

    @property
    def size_bytes(self) -> int:  # File size in the KBC Storage (read only input attribute)
        return self._size_bytes

    @property
    def max_age_days(self) -> int:  # File max age (read only input attribute)
        return self._max_age_days


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
    source: str = ''
    destination: str = None
    limit: int = None
    columns: List[str] = dataclasses.field(default_factory=lambda: [])
    where_values: List[str] = None
    full_path: str = None
    where_operator: str = ''
    days: int = 0
    column_types: List[TableColumnTypes] = None


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
    data: dict
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
