# Python 3.7 support
from __future__ import annotations
import dataclasses
import json
import logging
import warnings
from abc import ABC, abstractmethod
from collections import OrderedDict
from dataclasses import dataclass, field
from datetime import datetime
from enum import Enum
from pathlib import Path
from typing import List, Union, Dict, Optional, OrderedDict as TypeOrderedDict


from deprecated import deprecated

from .exceptions import UserException

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

        if manifest.get('schema') and (
                manifest.get('metadata') or manifest.get('column_metadata') or manifest.get('columns')):  # noqa
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

    def get_table_metadata_for_manifest(self, legacy_manifest: bool = False) -> List[dict]:
        """
        Returns table metadata list as required by the
        [manifest format]
        (https://developers.keboola.com/extend/common-interface/manifest-files/#dataintables-manifests)

        e.g.
        tm = TableMetadata()
        manifest['metadata'] = tm.table_metadata

        Returns: List[dict]

        """
        if legacy_manifest:
            final_metadata_list = [{'key': key,
                                    'value': self.table_metadata[key]}
                                   for key in self.table_metadata]
        else:
            final_metadata_list = [{key: self.table_metadata[key]}
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

        return self._get_legacy_column_metadata_for_manifest()

    def _get_legacy_column_metadata_for_manifest(self) -> dict:
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
    dtype: str
    length: Optional[str] = None
    default: Optional[str] = None

    def __post_init__(self):
        if isinstance(self.dtype, SupportedDataTypes):
            self.dtype = self.dtype.value


class BaseType(dict):
    def __init__(self, dtype: SupportedDataTypes = SupportedDataTypes.STRING, length: Optional[str] = None,
                 default: Optional[str] = None):
        super().__init__(base=DataType(dtype=dtype, length=length, default=default))

    @classmethod
    def string(cls, length: Optional[str] = None, default: Optional[str] = None) -> 'BaseType':
        return BaseType(dtype=SupportedDataTypes.STRING, length=length, default=default)

    @classmethod
    def integer(cls, length: Optional[str] = None,
                default: Optional[str] = None) -> 'BaseType':
        return BaseType(dtype=SupportedDataTypes.INTEGER, length=length, default=default)

    @classmethod
    def numeric(cls, length: Optional[str] = None,
                default: Optional[str] = None) -> 'BaseType':
        return BaseType(dtype=SupportedDataTypes.NUMERIC, length=length, default=default)

    @classmethod
    def float(cls, length: Optional[str] = None,
              default: Optional[str] = None) -> 'BaseType':
        return BaseType(dtype=SupportedDataTypes.FLOAT, length=length, default=default)

    @classmethod
    def boolean(cls, default: Optional[str] = None) -> 'BaseType':
        return BaseType(dtype=SupportedDataTypes.BOOLEAN, default=default)

    @classmethod
    def date(cls, default: Optional[str] = None) -> 'BaseType':
        return BaseType(dtype=SupportedDataTypes.DATE, default=default)

    @classmethod
    def timestamp(cls, default: Optional[str] = None) -> 'BaseType':
        return BaseType(dtype=SupportedDataTypes.TIMESTAMP, default=default)


@dataclass
class ColumnDefinition:
    """
    Represents the definition of a column within a table schema.

    Attributes:
        name (Optional[str]): The name of the column. Defaults to None.
        data_types (Optional[Union[Dict[str, DataType], BaseType]]): Data types of the column for specified backend.
        This can be a specific `DataType` or a `BaseType`, or a dictionary mapping from a string to one of these types.
        Defaults to BaseType.String.
        nullable (Optional[bool]): A flag indicating if the column can contain NULL values. Defaults to True.
        primary_key (Optional[bool]): Indicating if the column is part of the table's primary key. Defaults to False.
        description (Optional[str]): A description of the column's purpose or contents. Defaults to None.
        metadata (Optional[Dict[str, str]]): Additional metadata associated with the column. Defaults to None.
    """
    data_types: Optional[Union[Dict[str, DataType], BaseType]] = field(default_factory=lambda: BaseType())
    nullable: Optional[bool] = True
    primary_key: Optional[bool] = False
    description: Optional[str] = None
    metadata: Optional[Dict[str, str]] = None

    def update_properties(self, **kwargs):
        for key, value in kwargs.items():
            if hasattr(self, key):
                setattr(self, key, value)
            else:
                raise AttributeError(f"{key} is not a valid attribute of {self.__class__.__name__}")

    def from_dict(self, col: dict):
        return ColumnDefinition(
            data_types={key: DataType(dtype=v.get('type'), default=v.get('default'), length=v.get('length'))
                        for key, v in col.get('data_type', {}).items()},
            nullable=col.get('nullable'),
            primary_key=col.get('primary_key'),
            description=col.get('description'),
            metadata=col.get('metadata'))

    def add_datatype(self, backend: str, data_type: DataType):
        if backend in self.data_types:
            raise ValueError(f"Data type for backend {backend} already exists, use update_datatype instead")
        self.data_types[backend] = data_type

    def update_datatype(self, backend: str, data_type: DataType):
        if backend not in self.data_types:
            raise ValueError(f"Data type for backend {backend} does not exist, use add_datatype instead")
        self.data_types[backend] = data_type

    def to_dict(self, name: str):
        # convert datatypes to dict
        datatypes_dict = {}
        for key, value in self.data_types.items():
            datatypes_dict[key] = dataclasses.asdict(value)

        datatypes_dict = {key: {k.replace('dtype', 'type'): v for k, v in value.items()}
                          for key, value in datatypes_dict.items()}

        result = {
            'name': name,
            'data_type': datatypes_dict,
            'nullable': self.nullable,
            'primary_key': self.primary_key,
            'description': self.description,
            'metadata': self.metadata
        }
        # TODO: tohle bych delal az pri zapisu manifestu celkove, chceme vyhodit None values, false nechat
        filtered = {k: v for k, v in result.items() if v not in [False]}

        return filtered


@dataclass
class SupportedManifestAttributes(SubscriptableDataclass):
    out_attributes: List[str]
    in_attributes: List[str]
    out_legacy_exclude: List[str] = dataclasses.field(default_factory=lambda: [])
    in_legacy_exclude: List[str] = dataclasses.field(default_factory=lambda: [])

    def get_attributes_by_stage(self, stage: Literal['in', 'out'], legacy_queue: bool = False,
                                legacy_manifest: bool = False) -> List[str]:
        if stage == 'out':
            attributes = self.out_attributes
            exclude = self.out_legacy_exclude

            if not legacy_manifest:
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

    @classmethod
    def build_from_manifest(cls,
                            manifest_file_path: str
                            ):
        raise NotImplementedError

    def _filter_attributes_by_manifest_type(self, manifest_type: Literal["in", "out"], legacy_queue: bool = False,
                                            native_types: bool = False):
        raise NotImplementedError

    def get_manifest_dictionary(self, manifest_type: Optional[str] = None, legacy_queue: bool = False,
                                legacy_manifest: Optional[bool] = None) -> dict:
        raise NotImplementedError

    @property
    def stage(self) -> str:
        """
        Helper property marking the stage of the file. (str)
        """
        return self._stage

    @stage.setter
    def stage(self, stage: str):
        if stage not in ['in', 'out']:
            raise ValueError(f'Invalid stage "{stage}", supported values are: "in", "out"')
        self._stage = stage

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
        has_header: True if the file has a header
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
        "rows_count",
        "data_size_bytes",
        "is_alias",
        "attributes",
        "indexed_columns"
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
                 full_path: Optional[Union[str, None]] = None,
                 is_sliced: Optional[bool] = False,
                 destination: Optional[str] = '',
                 primary_key: Optional[List[str]] = None,
                 schema: Optional[Union[TypeOrderedDict[str, ColumnDefinition], list[str]]] = None,
                 incremental: Optional[bool] = None,
                 table_metadata: Optional[TableMetadata] = None,
                 enclosure: Optional[str] = '"',
                 delimiter: Optional[str] = ',',
                 delete_where: Optional[dict] = None,
                 stage: Optional[str] = 'out',
                 write_always: Optional[bool] = False,
                 has_header: Optional[bool] = None,
                 # input
                 **kwargs
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
            has_header: True if the file has a header, if emtpy inferred.
            destination: String name of the table in Storage.
            primary_key: List with names of columns used for primary key.
            incremental: Set to true to enable incremental loading
            table_metadata: <.dao.TableMetadata> object containing column and table metadata (deprecated)
            enclosure: str: CSV enclosure, by default "
            delimiter: str: CSV delimiter, by default ,
            delete_where (dict): Dict with settings for deleting rows
            stage: str: Storage Stage 'in' or 'out'
            write_always: Bool: If true, the table will be saved to Storage even when the job execution
                           fails.
            schema: (dict|lis[str]) Mapping of column names andColumnDefinition objects, or a list of names

        """
        super().__init__(full_path)
        self._name = name
        self.is_sliced = is_sliced

        # initialize manifest properties
        self._destination = None
        self.destination = destination
        self._schema = dict()

        if schema:
            self.schema = schema
        # deprecated argument for backward compatibility
        self._legacy_mode = False
        if kwargs.get('force_legacy_mode'):
            self._legacy_mode = True
        if kwargs.get('columns'):
            self.columns = kwargs['columns']

        self._legacy_primary_key = list()
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
        self.write_always = write_always

        # input manifest properties
        self._id = kwargs.get('id')
        self._uri = kwargs.get('uri')
        self._created = kwargs.get('created')
        self._last_change_date = kwargs.get('last_change_date')
        self._last_import_date = kwargs.get('last_import_date')
        self._rows_count = kwargs.get('rows_count')
        self._data_size_bytes = kwargs.get('data_size_bytes')
        self._is_alias = kwargs.get('is_alias')
        self._indexed_columns = kwargs.get('indexed_columns')
        self._attributes = kwargs.get('attributes')

        self.stage = stage
        self.has_header = has_header or self._has_header_in_file()

    def __get_stage_inferred(self):
        if self._uri:
            return 'in'
        return 'out'

    @classmethod
    def build_output_definition(cls, name: str,
                                destination: Optional[str] = '',
                                columns: Optional[List[str]] = None,
                                primary_key: Optional[List[str]] = None,
                                incremental: Optional[bool] = False,
                                table_metadata: Optional[TableMetadata] = None,
                                enclosure: Optional[str] = '"',
                                delimiter: Optional[str] = ',',
                                delete_where: Optional[dict] = None,
                                write_always: Optional[bool] = False,
                                schema: Optional[List[ColumnDefinition]] = None,
                                ):
        """
        Factory method for creating a TableDefinition instance for output tables.

        This method initializes a TableDefinition object with properties specific to output tables,
        including metadata and schema definitions.

        Args:
            name (str): The name of the table.
            destination (Optional[str]): The destination table name in the storage. Defaults to an empty string.
            columns (Optional[List[str]]): A list of column names for the table. Defaults to None.
            primary_key (Optional[List[str]]): A list of column names that form the primary key. Defaults to None.
            incremental (Optional[bool]): Indicates if the loading should be incremental. Defaults to False.
            table_metadata (Optional[TableMetadata]): An object containing table and column metadata. Defaults to None.
            enclosure (Optional[str]): The character used as a text qualifier in the CSV file. Defaults to '"'.
            delimiter (Optional[str]): The character used to separate columns in the CSV file. Defaults to ','.
            delete_where (Optional[dict]): Criteria for row deletion in incremental loads. Defaults to None.
            write_always (Optional[bool]): If True, the table will be saved to storage even if the job fails.
            schema (Optional[List[ColumnDefinition]]): Dictionary of ColumnDefinition objects.

        Returns:
            TableDefinition: An instance of TableDefinition configured for output tables.
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
                               full_path: Optional[Union[str, None]] = None,
                               is_sliced: Optional[bool] = False,

                               destination: Optional[str] = '',
                               primary_key: Optional[List[str]] = None,
                               columns: Optional[List[str]] = None,
                               incremental: Optional[bool] = None,
                               table_metadata: Optional[TableMetadata] = None,
                               enclosure: Optional[str] = '"',
                               delimiter: Optional[str] = ',',
                               delete_where: Optional[dict] = None,
                               stage: Optional[str] = 'in',
                               write_always: Optional[bool] = False,
                               schema: Optional[List[ColumnDefinition]] = None,
                               rows_count: Optional[int] = None,
                               data_size_bytes: Optional[int] = None,
                               is_alias: Optional[bool] = False,

                               # input
                               uri: Optional[str] = None,
                               id: Optional[str] = '',
                               created: Optional[str] = None,
                               last_change_date: Optional[str] = None,
                               last_import_date: Optional[str] = None):
        """
        Factory method for creating a TableDefinition instance for input tables.

        This method initializes a TableDefinition object with properties specific to input tables,
        including metadata and schema definitions.

        Args:
            name (str): The name of the table.
            full_path (Optional[Union[str, None]]): The full path to the table file or folder (for sliced tables).
            is_sliced (Optional[bool]): Indicates if the table is sliced (stored in multiple files).
            destination (Optional[str]): The destination table name in the storage. Defaults to an empty string.
            primary_key (Optional[List[str]]): A list of column names that form the primary key. Defaults to None.
            columns (Optional[List[str]]): A list of column names for the table. Defaults to None.
            incremental (Optional[bool]): Indicates if the loading should be incremental. Defaults to None.
            table_metadata (Optional[TableMetadata]): An object containing table and column metadata. Defaults to None.
            enclosure (Optional[str]): The character used as a text qualifier in the CSV file. Defaults to '"'.
            delimiter (Optional[str]): The character used to separate columns in the CSV file. Defaults to ','.
            delete_where (Optional[dict]): Criteria for row deletion in incremental loads. Defaults to None.
            stage (Optional[str]): Indicates the stage ('in' for input tables). Defaults to 'in'.
            write_always (Optional[bool]): If True, the table will be saved to storage even if the job fails. Defaults to False.  # noqa
            schema (Optional[List[ColumnDefinition]]): A list of ColumnDefinition objects defining the table schema. Defaults to None.  # noqa
            rows_count (Optional[int]): The number of rows in the table. Defaults to None.
            data_size_bytes (Optional[int]): The size of the table data in bytes. Defaults to None.
            is_alias (Optional[bool]): Indicates if the table is an alias. Defaults to False.
            uri (Optional[str]): The URI of the table. Defaults to None.
            id (Optional[str]): The ID of the table. Defaults to an empty string.
            created (Optional[str]): The creation timestamp of the table. Defaults to None.
            last_change_date (Optional[str]): The last modification timestamp of the table. Defaults to None.
            last_import_date (Optional[str]): The last import timestamp of the table. Defaults to None.

        Returns:
            TableDefinition: An instance of TableDefinition configured for input tables.
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
        data_type = {'base': DataType(dtype='STRING')}
        nullable = True
        for item in column_metadata:
            if item['key'] == 'KBC.datatype.basetype':
                data_type = {'base': DataType(dtype=item['value'])}
            elif item['key'] == 'KBC.datatype.nullable':
                nullable = item['value']
        return ColumnDefinition(data_types=data_type, nullable=nullable, primary_key=primary_key)

    @classmethod
    def return_schema_from_manifest(cls, json_data):
        if TableDefinition.is_new_manifest(json_data):
            schema = OrderedDict()
            for col in json_data.get('schema'):
                schema[col.get("name")] = ColumnDefinition().from_dict(col)

        else:
            # legacy support
            columns_metadata = json_data.get('column_metadata', {})
            primary_key = json_data.get('primary_key', [])
            columns = json_data.get('columns', [])

            all_columns = columns
            schema = OrderedDict()

            for col in all_columns:
                pk = col in primary_key
                if col in columns_metadata:
                    schema[col] = cls.convert_to_column_definition(col, columns_metadata[col], primary_key=pk)
                else:
                    schema[col] = ColumnDefinition(data_types={"base": DataType(dtype="STRING")}, primary_key=pk)

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

        # test if the manifest is output and incompatible
        force_legacy_mode = False
        if not manifest.get('columns') and manifest.get('primary_key'):
            warnings.warn('Primary key is set but columns are not. Forcing legacy mode for CSV file.',
                          DeprecationWarning)
            force_legacy_mode = True

        if manifest.get('id'):
            stage = 'in'
        else:
            stage = 'out'

        table_def = cls(name=name,
                        stage=stage,
                        full_path=full_path,
                        is_sliced=is_sliced,
                        id=manifest.get('id'),
                        table_metadata=TableMetadata(manifest),
                        primary_key=manifest.get('primary_key'),
                        schema=cls.return_schema_from_manifest(manifest),
                        uri=manifest.get('uri'),
                        created=manifest.get('created'),
                        last_change_date=manifest.get('last_change_date'),
                        last_import_date=manifest.get('last_import_date'),
                        rows_count=manifest.get('rows_count'),
                        data_size_bytes=manifest.get('data_size_bytes'),
                        is_alias=manifest.get('is_alias'),
                        force_legacy_mode=force_legacy_mode,
                        indexed_columns=manifest.get('indexed_columns'),
                        attributes=manifest.get('attributes')
                        )

        return table_def

    def get_manifest_dictionary(self, manifest_type: Optional[str] = None, legacy_queue: bool = False,
                                legacy_manifest: Optional[bool] = None) -> dict:
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
            legacy_manifest (bool): If True, creates a legacy manifest; otherwise, uses the new format if permitted.

        Returns:
            dict representation of the manifest file in a format expected / produced by the Keboola Connection

        """

        if not manifest_type:
            manifest_type = self.stage

        if self._legacy_mode:
            legacy_manifest = True
        dictionary = self._filter_attributes_by_manifest_type(manifest_type, legacy_queue, legacy_manifest)

        filtered_dictionary = self._filter_dictionary(dictionary)

        return filtered_dictionary

    def _filter_dictionary(self, data):
        if isinstance(data, dict):
            return {
                k: self._filter_dictionary(v)
                for k, v in data.items()
                if v not in (None, [], {}, "")
            }
        elif isinstance(data, list):
            return [self._filter_dictionary(item) for item in data if item not in (None, [], {}, "")]
        else:
            return data

    # Usage

    def _filter_attributes_by_manifest_type(self, manifest_type: Literal["in", "out"], legacy_queue: bool = False,
                                            legacy_manifest: bool = False):
        """
        Filter manifest to contain only supported fields
        Args:
            manifest_type:

        Returns:

        """

        supported_fields = self._manifest_attributes.get_attributes_by_stage(manifest_type, legacy_queue,
                                                                             legacy_manifest)
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
            'indexed_columns': self._indexed_columns,
            'attributes': self._attributes,

            'destination': self.destination,
            'incremental': self.incremental,
            'primary_key': self.primary_key,
            'write_always': self.write_always,
            'delimiter': self.delimiter,
            'enclosure': self.enclosure,
            'metadata': self.table_metadata.get_table_metadata_for_manifest(legacy_manifest=True),
            'column_metadata': self.table_metadata._get_legacy_column_metadata_for_manifest(),
            'manifest_type': manifest_type,
            'has_header': self.has_header,
            'description': None,
            'table_metadata': self.table_metadata.get_table_metadata_for_manifest(),
            'delete_where_column': self.delete_where_column,
            'delete_where_values': self.delete_where_values,
            'delete_where_operator': self.delete_where_operator,
            'schema': [col.to_dict(name)
                       for name, col in self.schema.items()] if isinstance(self.schema, (OrderedDict, dict)) else []
        }
        if legacy_manifest:
            fields['columns'] = self.column_names

        new_dict = fields.copy()

        if supported_fields:
            for attr in fields:
                if attr not in supported_fields:
                    new_dict.pop(attr, None)
        return new_dict

    def _has_header_in_file(self):
        if self.is_sliced:
            has_header = False
        elif self.column_names and not self.stage == 'in':
            has_header = False
        else:
            has_header = True
        return has_header

    @property
    def schema(self) -> TypeOrderedDict[str, ColumnDefinition]:
        return self._schema

    @schema.setter
    def schema(self, value: Union[TypeOrderedDict[str, ColumnDefinition], list[str]]):
        if value:
            if not isinstance(value, (list, dict, OrderedDict)):
                raise TypeError("Columns must be a list or a mapping of column names and ColumnDefinition objects")

            if isinstance(value, list):
                self._schema = OrderedDict()
                for col in value:
                    self._schema[col] = ColumnDefinition()
            else:
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
        if isinstance(self.schema, (OrderedDict, dict)):
            return list(self.schema.keys())
        else:
            return []

    @columns.setter
    @deprecated(version='1.5.1', reason="Please use new column_names method instead of schema property")
    def columns(self, val: List[str]):
        """
        Set columns for the table.
        If list of names provided, the columns will be created with default settings Basetype.String.
        Args:
            val:

        Returns:

        """
        if not isinstance(val, list):
            raise TypeError("Columns must be a list")

        self.schema = val

    @property
    def column_names(self) -> List[str]:
        if self.schema:
            return list(self.schema.keys())
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
        if not self._legacy_mode:
            return [column_name for column_name, column_def in self.schema.items() if column_def.primary_key]
        else:
            return self._legacy_primary_key

    @primary_key.setter
    def primary_key(self, primary_key: List[str]):
        if not primary_key:
            return

        if not isinstance(primary_key, list):
            raise TypeError("Primary key must be a list")
        if not self._legacy_mode:
            for col in primary_key:
                if col in self.schema:
                    self.schema[col].primary_key = True
                else:
                    raise UserException(f"Primary key column {col} not found in schema. "
                                        f"Please specify all columns / schema")
        else:
            self._legacy_primary_key = primary_key

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
        # backward compatibility legacy support
        for col, val in table_metadata._get_legacy_column_metadata_for_manifest().items():
            if not self.schema.get(col):
                self.schema[col] = ColumnDefinition()
            self.schema[col].metadata = {item['key']: item['value'] for item in val}

    @property
    def created(self) -> Union[datetime, None]:  # Created timestamp  in the KBC Storage (read only input attribute)
        if self._created:
            return datetime.strptime(self._created, KBC_DEFAULT_TIME_FORMAT)
        else:
            return None

    @property
    def uri(self) -> str:
        return self._uri

    @property
    def last_change_date(self) -> str:
        return self._last_change_date

    @property
    def last_import_date(self) -> str:
        return self._last_import_date

    @property
    def is_alias(self) -> bool:
        return self._is_alias

    def add_column(self, name: str, definition: ColumnDefinition = ColumnDefinition()):
        """
        Add column definition, accepts either ColumnDefinition or a string
        (in which case the base type STRING will be used).
        """

        if name in self._schema:
            raise ValueError(f"Column with name '{name}' already exists")

        self._schema[name] = definition

    def update_column(self, name: str, column_definition: ColumnDefinition):
        if not isinstance(column_definition, ColumnDefinition):
            raise ValueError("New column must be an instance of ColumnDefinition")

        if name in self.schema:
            self.schema[name] = column_definition
        else:
            raise ValueError(f'Column with name: "{name}" not found')

    def delete_column(self, column_name: str):

        if column_name not in self.schema:
            raise ValueError(f"Column with name {column_name} not found")
        del self.schema[column_name]

    def add_columns(self, columns: Union[List[str], Dict[str, ColumnDefinition]]):
        if isinstance(columns, list):
            for name in columns:
                self.add_column(name)
        else:
            for name, column in columns.items():
                self.add_column(name, column)

    def update_columns(self, columns: Dict[str, ColumnDefinition]):
        for name, column in columns:
            self.update_column(name, column)

    def delete_columns(self, column_names: List[str]):
        for name in column_names:
            self.delete_column(name)

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
                 stage: Optional[str] = 'out',
                 tags: Optional[List[str]] = None,
                 is_public: Optional[bool] = False,
                 is_permanent: Optional[bool] = False,
                 is_encrypted: Optional[bool] = False,
                 notify: Optional[bool] = False,
                 id: Optional[str] = None,
                 s3: Optional[dict] = None,
                 abs: Optional[dict] = None,
                 created: Optional[str] = None,
                 size_bytes: Optional[int] = None,
                 max_age_days: Optional[int] = None
                 ):
        """

        Args:
            full_path (str): Full path of the file.
            stage (str): Storage Stage 'in' or 'out' default out
            tags (list):
                List of tags that are assigned to this file
            is_public: When true, the file URL will be permanent and publicly accessible.
            is_permanent: Keeps a file forever. If false, the file will be deleted after default period of time (e.g.
            15 days)
            is_encrypted: If true, the file content will be encrypted in the storage.
            notify: Notifies project administrators that a file was uploaded.
        """
        super().__init__(full_path)

        self.stage = stage

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
    def build_output_definition(cls,
                                full_path: str,
                                tags: Optional[List[str]] = None,
                                is_public: Optional[bool] = False,
                                is_permanent: Optional[bool] = False,
                                is_encrypted: Optional[bool] = False,
                                notify: Optional[bool] = False):
        """
        Factory method to create an instance of FileDefinition for output files.

        This method initializes a FileDefinition object with properties specific to output files,
        including file path, tags, and various flags indicating the file's accessibility, permanence, encryption status,
        and whether project administrators should be notified upon file upload.

        Args:
            full_path (str): The full path where the file is or will be stored.
            tags (Optional[List[str]]): A list of tags associated with the file. Defaults to None.
            is_public (Optional[bool]): Flag indicating if the file URL will be permanent and publicly accessible. Defaults to False.  # noqa
            is_permanent (Optional[bool]): Flag indicating if the file should be kept forever. Defaults to False.
            is_encrypted (Optional[bool]): Flag indicating if the file content will be encrypted in storage. Defaults to False.  # noqa
            notify (Optional[bool]): Flag indicating if project administrators should be notified that a file was uploaded. Defaults to False.  # noqa

        Returns:
            An instance of FileDefinition configured for output files.
        """
        return cls(full_path=full_path, stage="out", tags=tags, is_public=is_public, is_permanent=is_permanent,
                   is_encrypted=is_encrypted, notify=notify)

    @classmethod
    def build_input_definition(cls, full_path: str,
                               id: Optional[str] = None,
                               s3: Optional[dict] = None,
                               abs: Optional[dict] = None,
                               created: Optional[str] = None,
                               size_bytes: Optional[int] = None,
                               max_age_days: Optional[int] = None):
        """
        Factory method to create an instance of FileDefinition for input files.

        This method initializes a FileDefinition object with properties specific to input files,
        including the file path, optional metadata such as the file's ID, S3 and ABS storage details,
        creation date, size in bytes, and the maximum age in days before the file is considered expired.

        Args:
            full_path (str): The full path where the file is or will be stored.
            id (Optional[str]): The unique identifier of the file. Defaults to None.
            s3 (Optional[dict]): A dictionary containing Amazon S3 storage details. Defaults to None.
            abs (Optional[dict]): A dictionary containing Azure Blob Storage details. Defaults to None.
            created (Optional[str]): The creation date of the file. Defaults to None.
            size_bytes (Optional[int]): The size of the file in bytes. Defaults to None.
            max_age_days (Optional[int]): The maximum age of the file in days. Defaults to None.

        Returns:
            An instance of FileDefinition configured for input files.
        """
        return cls(full_path=full_path, stage="in", id=id, s3=s3, abs=abs, created=created, size_bytes=size_bytes,
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

        if manifest.get('id'):
            stage = 'in'
        else:
            stage = 'out'

        file_def = cls(full_path=full_path,
                       stage=stage,
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

    def get_manifest_dictionary(self, manifest_type: Optional[str] = None, legacy_queue: bool = False,
                                legacy_manifest: Optional[bool] = None) -> dict:
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
            legacy_manifest (bool): If True, creates a legacy manifest; otherwise, uses the new format if permitted.

        Returns:
            dict representation of the manifest file in a format expected / produced by the Keboola Connection

        """
        if not manifest_type:
            manifest_type = self.stage

        dictionary = self._filter_attributes_by_manifest_type(manifest_type, legacy_queue, legacy_manifest)

        filtered_dictionary = {k: v for k, v in dictionary.items() if v not in [None, [], {}, ""]}

        return filtered_dictionary

    def _filter_attributes_by_manifest_type(self, manifest_type: Literal["in", "out"], legacy_queue: bool = False,
                                            legacy_manifest: bool = False):
        """
        Filter manifest to contain only supported fields
        Args:
            manifest_type:

        Returns:

        """

        if manifest_type == 'in':

            manifest_dictionary = {
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

        else:
            manifest_dictionary = {
                'is_public': self.is_public,
                'is_permanent': self.is_permanent,
                'is_encrypted': self.is_encrypted,
                'tags': self.tags,
                'notify': self.notify,
            }

        return manifest_dictionary

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
