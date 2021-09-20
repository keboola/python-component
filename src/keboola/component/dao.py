import dataclasses
import json
from abc import ABC, abstractmethod
from dataclasses import dataclass
from datetime import datetime
from enum import Enum
from pathlib import Path
from typing import List, Union, Dict, Optional

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

    def get_table_metadata_for_manifest(self) -> List[dict]:
        """
        Returns table metadata list as required by the
        [manifest format]
        (https://developers.keboola.com/extend/common-interface/manifest-files/#dataintables-manifests)

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
        if errors:
            raise ValueError(', '.join(errors) + f'\n Supported base types are: [{SupportedDataTypes.list()}]')


class IODefinition(ABC):

    def __init__(self, full_path):
        self._raw_manifest: dict = dict()
        self.full_path = full_path

        # infer stage by default
        self.__stage = self.__get_stage_inferred()

    @classmethod
    def build_from_manifest(cls,
                            manifest_file_path: str
                            ):
        raise NotImplementedError

    def _filter_attributes_by_manifest_type(self, manifest_type):
        """
        Filter manifest to contain only supported fields
        Args:
            manifest_type:

        Returns:

        """
        supported_fields = self._manifest_attributes.get(manifest_type, [])
        new_dict = self._raw_manifest.copy()
        if supported_fields:
            for attr in self._raw_manifest:
                if attr not in supported_fields:
                    new_dict.pop(attr, None)
        return new_dict

    def get_manifest_dictionary(self, manifest_type: Optional[str] = None) -> dict:
        """
        Returns manifest dictionary in appropriate manifest_type: either 'in' or 'out'.
        By default returns output manifest.
             The result keeps only values that are applicable for
             the selected type of the Manifest file. Because although input and output manifests share most of
             the attributes, some are not shared.

             See [manifest files](https://developers.keboola.com/extend/common-interface/manifest-files)
             for more information.

        Args:
            manifest_type (str): either 'in' or 'out'.
             See [manifest files](https://developers.keboola.com/extend/common-interface/manifest-files)
             for more information.

        Returns:
            dict representation of the manifest file in a format expected / produced by the Keboola Connection

        """
        if not manifest_type:
            manifest_type = self.stage

        return self._filter_attributes_by_manifest_type(manifest_type)

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
    def _manifest_attributes(self) -> Dict[str, List[str]]:
        """
        Manifest attributes
        """
        return {}

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
        s3 = self._raw_manifest.get('s3')
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
        _abs = self._raw_manifest.get('abs')
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
        "column_metadata"
    ]

    OUTPUT_MANIFEST_ATTRIBUTES = [
        "destination",
        "columns",
        "incremental",
        "primary_key",
        "delimiter",
        "enclosure",
        "metadata",
        "column_metadata",
        "delete_where_column",
        "delete_where_values",
        "delete_where_operator"
    ]

    MANIFEST_ATTRIBUTES = {'in': INPUT_MANIFEST_ATTRIBUTES,
                           'out': OUTPUT_MANIFEST_ATTRIBUTES}

    def __init__(self, name: str, full_path: Union[str, None] = None, is_sliced: bool = False,
                 destination: str = '',
                 primary_key: List[str] = None,
                 columns: List[str] = None,
                 incremental: bool = None,
                 table_metadata: TableMetadata = None,
                 delete_where: dict = None):
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
            delete_where (dict): Dict with settings for deleting rows
        """
        super().__init__(full_path)
        self._name = name
        self.is_sliced = is_sliced
        self._raw_manifest = dict()

        # initialize manifest properties
        self.destination = destination
        self.primary_key = primary_key
        self.columns = columns
        self.incremental = incremental

        if not table_metadata:
            table_metadata = TableMetadata()
        self.table_metadata = table_metadata
        self.set_delete_where_from_dict(delete_where)

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

        table_def = cls(name=name, full_path=full_path,
                        is_sliced=is_sliced, table_metadata=TableMetadata(manifest))
        # build manifest definition
        table_def._raw_manifest = manifest

        return table_def

    @property
    def _manifest_attributes(self) -> Dict[str, List[str]]:
        return self.MANIFEST_ATTRIBUTES

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
    def id(self) -> str:
        """
        str: id property used in input manifest. Contains Keboola Storage ID, e.g. in.c-bucket.table

        """
        return self._raw_manifest.get('id', '')

    @id.setter
    def id(self, val: str):
        if val:
            if isinstance(val, str):
                self._raw_manifest['id'] = val
            else:
                raise TypeError("ID must be a string")

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
        return self._raw_manifest.get('rows_count', '')

    @rows_count.setter
    def rows_count(self, val: int):
        if val:
            if isinstance(val, int):
                self._raw_manifest['rows_count'] = val
            else:
                raise TypeError("ID must be a int")

    @property
    def data_size_bytes(self) -> int:
        """
                int: data_size_bytes property used in input manifest.

        """
        return self._raw_manifest.get('data_size_bytes', '')

    @data_size_bytes.setter
    def data_size_bytes(self, val: int):
        if val:
            if isinstance(val, int):
                self._raw_manifest['data_size_bytes'] = val
            else:
                raise TypeError("data_size_bytes must be a int")

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

    def get_manifest_dictionary(self, stage_type: Optional[str] = None) -> dict:
        """

        Args:
             See [manifest files](https://developers.keboola.com/extend/common-interface/manifest-files)
             for more information.

        Returns:
            dict representation of the manifest file in a format expected / produced by the Keboola Connection

        """
        # in case the table_metadata is out of sync, e.g. the object was modified in-place
        self._set_table_metadata_to_manifest(self._table_metadata)
        super(TableDefinition, self).get_manifest_dictionary(stage_type)
        return self._raw_manifest


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
                 notify: bool = False):
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

        file_def = cls(full_path=full_path)
        # build manifest definition
        file_def._raw_manifest = manifest

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
        if self._raw_manifest.get('id'):
            fsplit = file_name.split('_', 1)
            if len(fsplit) > 1:
                self._raw_manifest['id'] = fsplit[0]
                file_name = fsplit[1]
        return file_name

    @property
    def full_name(self):
        """
        File name - full file name, directly from the path. Includes the KBC generated ID. (`str`, read-only)
        """
        return Path(self.full_path).name

    @property
    def _manifest_attributes(self) -> Dict[str, List[str]]:
        return {'out': self.OUTPUT_MANIFEST_KEYS}

    # ########### Output manifest properties - R/W

    @property
    def user_tags(self) -> List[str]:
        """
        User defined tags excluding the system tags
        """
        # filter system tags
        tags: List[str] = [tag for tag in self._raw_manifest.get('tags', []) if not self.is_system_tag(tag)]
        return tags

    @property
    def tags(self) -> List[str]:
        """
        All tags specified on the file
        """
        return self._raw_manifest.get('tags', [])

    @tags.setter
    def tags(self, tags: List[str]):
        if tags is None:
            tags = list()
        self._raw_manifest['tags'] = tags

    @property
    def is_public(self) -> bool:
        return self._raw_manifest.get('is_public', False)

    @is_public.setter
    def is_public(self, is_public: bool):
        self._raw_manifest['is_public'] = is_public

    @property
    def is_permanent(self) -> bool:
        return self._raw_manifest.get('is_permanent', False)

    @is_permanent.setter
    def is_permanent(self, is_permanent: bool):
        self._raw_manifest['is_permanent'] = is_permanent

    @property
    def is_encrypted(self) -> bool:
        return self._raw_manifest.get('is_encrypted', False)

    @is_encrypted.setter
    def is_encrypted(self, is_encrypted: bool):
        self._raw_manifest['is_encrypted'] = is_encrypted

    @property
    def notify(self) -> bool:
        return self._raw_manifest.get('notify', False)

    @notify.setter
    def notify(self, notify: bool):
        self._raw_manifest['notify'] = notify

    # ########### Input manifest properties - Read ONLY
    @property
    def id(self) -> str:  # File ID in the KBC Storage (read only input attribute)
        return self._raw_manifest.get('id', None)

    @property
    def created(self) -> Union[datetime, None]:  # Created timestamp  in the KBC Storage (read only input attribute)
        if self._raw_manifest.get('created'):
            return datetime.strptime(self._raw_manifest['created'], KBC_DEFAULT_TIME_FORMAT)
        else:
            return None

    @property
    def size_bytes(self) -> int:  # File size in the KBC Storage (read only input attribute)
        return self._raw_manifest.get('size_bytes', 0)

    @property
    def max_age_days(self) -> int:  # File max age (read only input attribute)
        return self._raw_manifest.get('max_age_days', 0)


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
