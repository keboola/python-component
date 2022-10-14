from typing import List, Dict
from typing import Optional, Union
from keboola.component.dao import SupportedDataTypes
from dataclasses import dataclass


@dataclass
class FieldSchema:
    """
    Defines the name and type specifications of a single field in a table
    """
    name: str
    base_type: Optional[Union[SupportedDataTypes, str]] = None
    description: Optional[str] = None
    nullable: bool = False
    length: Optional[str] = None
    default: Optional[str] = None


@dataclass
class TableSchema:
    """
    TableSchema class is used to define the schema and metadata of a table.
    """
    name: str
    fields: List[FieldSchema]
    primary_keys: Optional[List[str]] = None
    parent_tables: Optional[List[str]] = None
    description: Optional[str] = None

    @property
    def field_names(self) -> List[str]:
        return [column.name for column in self.fields]

    @property
    def csv_name(self) -> str:
        return f"{self.name}.csv"

    def add_field(self, new_field: FieldSchema) -> None:
        """
        Adds extra field to the tableschema.
        Args:
            new_field:  FieldSchema to add to the list of fields

        """
        self.fields.append(new_field)


def init_table_schema_from_dict(json_table_schema: Dict) -> TableSchema:
    """
    Function to initialize a Table Schema from a dictionary.
    Example of the json_table_schema structure:
    {
      "name": "product",
      "description": "this table holds data on products",
      "parent_tables": [],
      "primary_keys": [
        "id"
      ],
      "fields": [
        {
          "name": "id",
          "base_type": "string",
          "description": "ID of the product",
          "length": "100",
          "nullable": false
        },
        {
          "name": "name",
          "base_type": "string",
          "description": "Plain-text name of the product",
          "length": "1000",
          "default": "Default Name"
        }
      ]
    }
    """
    try:
        json_table_schema["fields"] = [FieldSchema(**field) for field in json_table_schema["fields"]]
    except TypeError as type_error:
        raise KeyError(
            f"When creating the table schema the definition of columns failed : {type_error}") from type_error
    try:
        ts = TableSchema(**json_table_schema)
    except TypeError as type_error:
        raise KeyError(
            f"When creating the table schema the definition of the table failed : {type_error}") from type_error
    return ts
