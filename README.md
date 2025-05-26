# Keboola Python Component library

## Introduction

![Build & Test](https://github.com/keboola/python-component/workflows/Build%20&%20Test/badge.svg?branch=main)
[![Code Climate](https://codeclimate.com/github/keboola/python-component/badges/gpa.svg)](https://codeclimate.com/github/keboola/python-component)
[![PyPI version](https://badge.fury.io/py/keboola.component.svg)](https://badge.fury.io/py/keboola.component)

This library provides a Python wrapper over the
[Keboola Common Interface](https://developers.keboola.com/extend/common-interface/). It simplifies all tasks related to
the communication of the [Docker component](https://developers.keboola.com/extend/component/) with the Keboola
Connection that is defined by the Common Interface. Such tasks are config manipulation, validation, component state, I/O
handling, I/O metadata and manifest files, logging, etc.

It is being developed by the Keboola Data Services team and officially supported by Keboola. It aims to simplify the
Keboola Component creation process, by removing the necessity of writing boilerplate code to manipulate with the Common
Interface.

Another useful use-case is within the Keboola [Python Transformations](https://help.keboola.com/transformations/python/)
to simplify the I/O handling.

### Links

- API Documentation: [API docs](https://keboola.github.io/python-component/interface.html)
- Source code: [https://github.com/keboola/python-component](https://github.com/keboola/python-component)
- PYPI project
  code: [https://test.pypi.org/project/keboola.component-kds/](https://test.pypi.org/project/keboola.component-kds/)
-

Documentation: [https://developers.keboola.com/extend/component/python-component-library](https://developers.keboola.com/extend/component/)

- Python Component Cookiecutter template
  project: [https://bitbucket.org/kds_consulting_team/cookiecutter-python-component](https://bitbucket.org/kds_consulting_team/cookiecutter-python-component)

# Quick start

## Installation

The package may be installed via PIP:

 ```
pip install keboola.component
```

## Core structure & functionality

The package contains two core modules:

- `keboola.component.interface` - Core methods and class to initialize and handle
  the [Keboola Common Interface](https://developers.keboola.com/extend/common-interface/) tasks
- `keboola.component.dao` - Data classes and containers for objects defined by the Common Interface such as manifest
  files, metadata, environment variables, etc.
- `keboola.component.base` - Base classes to build the Keboola Component applications from.

## CommonInterface

Core class that serves to initialize the docker environment. It handles the following tasks:

- Environment initialisation
    - Loading
      all [environment variables](https://developers.keboola.com/extend/common-interface/environment/#environment-variables)
    - Loading the [configuration file](https://developers.keboola.com/extend/common-interface/config-file/) and
      initialization of the [data folder](https://developers.keboola.com/extend/common-interface/folders/)
    - [State file](https://developers.keboola.com/extend/common-interface/config-file/#state-file) processing.
    - [Logging](https://developers.keboola.com/extend/common-interface/logging/)
- [Data folder](https://developers.keboola.com/extend/common-interface/folders/) manipulation
    - [Manifest file](https://developers.keboola.com/extend/common-interface/manifest-files/) processing
    - Config validation
    - Metadata manipulation
    - [OAuth](https://developers.keboola.com/extend/common-interface/oauth/) configuration handling.

## Initialization

The core class is `keboola.component.interface.CommonInterface`, upon it's initialization the environment is created.
e.g.

- data folder initialized (either from the Environment Variable or manually)
- config.json is loaded
- All Environment variables are loaded

The optional parameter `data_folder_path` of the constructor is the path to the data directory. If not
provided it will be determined in this order:
1.   [`KBC_DATADIR` environment variable](/extend/common-interface/environment/#environment-variables) if present
2. -d / --data argument from the command line if present
3. data folder inside the current working directory if present
4. data folder inside the parent directory of the current working directory if present

The class can be either extended or just instantiated and manipulated like object. The `CommonInterface` class is
exposed in the `keboola.component` namespace:

```python
from keboola.component import CommonInterface

# init the interface
# A ValueError error is raised if the KBC_DATADIR does not exist or contains non-existent path.
ci = CommonInterface()
```

To specify the data folder path manually use this code:

```python
from keboola.component import CommonInterface

# init the interface
# A ValueError error is raised if the data folder path does not exist.
ci = CommonInterface(data_folder_path='/data')
```

## Loading configuration parameters:

The below example loads initializes the common interface class and automatically loading config.json from the
[data folder](https://developers.keboola.com/extend/common-interface/folders/) which is defined by an environment
variable `KBC_DATADIR`, if the variable is not present, and error is raised. To override the data folder location
provide the `data_folder_path` parameter into constructor.

**NOTE:** The `configuration` object is initialized upon access and a ValueError is thrown if the `config.json` does not
exist in the data folder. e.g. `cfg = ci.configuration` may throw a ValueError even though the data folder exists and
ci (CommonInterface)
is properly initialized.

```python
from keboola.component import CommonInterface
# Logger is automatically set up based on the component setup (GELF or STDOUT)
import logging

SOME_PARAMETER = 'some_user_parameter'
REQUIRED_PARAMETERS = [SOME_PARAMETER]

# init the interface
# A ValueError error is raised if the KBC_DATADIR does not exist or contains non-existent path.
ci = CommonInterface()

# A ValueError error is raised if the config.json file does not exists in the data dir.
# Checks for required parameters and throws ValueError if any is missing.
ci.validate_configuration(REQUIRED_PARAMETERS)

# print KBC Project ID from the environment variable if present:
logging.info(ci.environment_variables.project_id)

# load particular configuration parameter
logging.info(ci.configuration.parameters[SOME_PARAMETER])
```

## Processing input tables - Manifest vs I/O mapping

Input and output tables specified by user are listed in the [configuration file](/extend/common-interface/config-file/).
Apart from that, all input tables provided by user also include manifest file with additional metadata.

Tables and their manifest files are represented by the `keboola.component.dao.TableDefinition` object and may be loaded
using the convenience method `get_input_tables_definitions()`. The result object contains all metadata about the table,
such as manifest file representations, system path and name.

### Manifest & input folder content

```python
from keboola.component import CommonInterface
import logging

# init the interface
ci = CommonInterface()

input_tables = ci.get_input_tables_definitions()

# print path of the first table (random order)
first_table = input_tables[0]
logging.info(f'The first table named: "{first_table.name}" is at path: {first_table.full_path}')

# get information from table manifest
logging.info(f'The first table has following columns defined in the manifest {first_table.column_names}')

```

### Using I/O mapping

```python
import csv
from keboola.component import CommonInterface

# initialize the library
ci = CommonInterface()

# get list of input tables from the input mapping ()
tables = ci.configuration.tables_input_mapping
j = 0
for table in tables:
    # get csv file name
    inName = table.destination

    # read input table manifest and get it's physical representation
    table_def = ci.get_input_table_definition_by_name(table.destination)

    # get csv file name with full path from output mapping
    outName = ci.configuration.tables_output_mapping[j].full_path

    # get file name from output mapping
    outDestination = ci.configuration.tables_output_mapping[j]['destination']
```

## I/O table manifests and processing results

The component may define
output [manifest files](https://developers.keboola.com/extend/common-interface/manifest-files/#dataouttables-manifests)
that define options on storing the results back to the Keboola Connection Storage. This library provides methods that
simplifies the manifest file creation and allows defining the export options and metadata of the result table using
helper objects `TableDefinition`
and `TableMetadata`.

`TableDefinition` object serves as a result container containing all the information needed to store the Table into the
Storage. It contains the manifest file representation and initializes all attributes available in the manifest.

This object represents both Input and Output manifests. All output manifest attributes are exposed in the class.

There are convenience methods for result processing and manifest creation `CommonInterface.write_manifest`. Also it is
possible to create the container for the output table using the `CommonInterface.create_out_table_definition()`.

![TableDefinition dependencies](docs/imgs/TableDefinition_class.png)

**Table schema example:**

```python
from keboola.component import CommonInterface
from keboola.component.dao import ColumnDefinition, DataType, SupportedDataTypes, BaseType

# init the interface
ci = CommonInterface()

# Create output table definition with schema
out_table = ci.create_out_table_definition(
    name="results.csv",                 # File name for the output
    destination="out.c-data.results",   # Destination table in Storage
    primary_key=["id"],                 # Primary key column(s)
    incremental=True                    # Enable incremental loading
)

# Define columns with their data types
out_table.add_columns([
    "id",                # Default type is STRING
    "created_at",        # Will add typed definition below
    "status",
    "value"
])

# Update column with specific data type - method 1 (using BaseType)
out_table.update_column("id", 
    ColumnDefinition(
        primary_key=True,  
        data_types=BaseType.integer()
    )
)

# Update column with specific data type - method 2 (using DataType)
out_table.update_column("created_at", 
    ColumnDefinition(
        data_types={"base": DataType(dtype=SupportedDataTypes.TIMESTAMP)}
    )
)

# Update column with specific data type - method 3 (backend-specific types)
out_table.update_column("value", 
    ColumnDefinition(
        data_types={
            "snowflake": DataType(dtype="NUMBER", length="38,2"),
            "bigquery": DataType(dtype="FLOAT64"),
            "base": DataType(dtype=SupportedDataTypes.NUMERIC, length="38,2")
        },
        description="Numeric value with 2 decimal places"
    )
)

# Write some data to the output file
import csv
with open(out_table.full_path, 'w', newline='') as f:
    writer = csv.DictWriter(f, fieldnames=out_table.column_names)
    writer.writeheader()
    writer.writerow({
        "id": "1",
        "created_at": "2023-01-15T14:30:00Z",
        "status": "completed",
        "value": "123.45"
    })
    
# Write manifest
ci.write_manifest(out_table)
```

**Simple Example for Basic Use Cases:**

```python
from keboola.component import CommonInterface
import csv

# Initialize the component
ci = CommonInterface()

# Create output table
result_table = ci.create_out_table_definition(
    'output.csv',
    primary_key=['id'],
    incremental=True,
    description='Data processed by my component'
)

# Write data to CSV
with open(result_table.full_path, 'w', newline='') as f:
    writer = csv.DictWriter(f, fieldnames=['id', 'name', 'value'])
    writer.writeheader()
    writer.writerow({"id": "1", "name": "Test", "value": "100"})
    writer.writerow({"id": "2", "name": "Example", "value": "200"})

# Write manifest file
ci.write_manifest(result_table)
```

### Get input table by name

```python
from keboola.component import CommonInterface

# init the interface
ci = CommonInterface()
table_def = ci.get_input_table_definition_by_name('input.csv')

```

## Working with Input/Output Mapping

Keboola Connection provides input/output mappings that define which tables are loaded into your component and which tables should be stored back. These mappings are defined in the configuration file and can be accessed programmatically.

### Accessing Input Tables from Mapping

```python
from keboola.component import CommonInterface
import csv

# Initialize the component
ci = CommonInterface()

# Access input mapping configuration
input_tables = ci.configuration.tables_input_mapping

# Process each input table
for table in input_tables:
    # Get the destination (filename in the /data/in/tables directory)
    table_name = table.destination
    
    # Load table definition from manifest
    table_def = ci.get_input_table_definition_by_name(table_name)
    
    # Print information about the table
    print(f"Processing table: {table_name}")
    print(f"  - Source: {table.source}")
    print(f"  - Full path: {table_def.full_path}")
    print(f"  - Columns: {table_def.column_names}")
    
    # Read data from the CSV file
    with open(table_def.full_path, 'r') as input_file:
        csv_reader = csv.DictReader(input_file)
        for row in csv_reader:
            # Process each row
            print(f"  - Row: {row}")
```

### Creating Output Tables based on Output Mapping

```python
from keboola.component import CommonInterface
import csv

# Initialize the component
ci = CommonInterface()

# Access output mapping configuration
output_tables = ci.configuration.tables_output_mapping

# Process each output table mapping
for i, table_mapping in enumerate(output_tables):
    # Get source (filename that should be created) and destination (where it will be stored in KBC)
    source = table_mapping.source
    destination = table_mapping.destination
    
    # Create output table definition
    out_table = ci.create_out_table_definition(
        name=source,
        destination=destination,
        incremental=table_mapping.incremental
    )
    
    # Add some sample data (in a real component, this would be your processed data)
    with open(out_table.full_path, 'w', newline='') as out_file:
        writer = csv.DictWriter(out_file, fieldnames=['id', 'data'])
        writer.writeheader()
        writer.writerow({'id': f'{i+1}', 'data': f'Data for {destination}'})
    
    # Write manifest file
    ci.write_manifest(out_table)
```

### Combining Input and Output Mapping

Here's a complete example that reads data from input tables and creates output tables:

```python
from keboola.component import CommonInterface
import csv

# Initialize the component
ci = CommonInterface()

# Get input tables
input_tables = ci.configuration.tables_input_mapping
output_tables = ci.configuration.tables_output_mapping

# Process each output table based on input
for i, out_mapping in enumerate(output_tables):
    # Find corresponding input table if possible (matching by index for simplicity)
    in_mapping = input_tables[i] if i < len(input_tables) else None
    
    # Create output table
    out_table = ci.create_out_table_definition(
        name=out_mapping.source,
        destination=out_mapping.destination,
        incremental=out_mapping.incremental
    )
    
    # If we have an input table, transform its data
    if in_mapping:
        in_table = ci.get_input_table_definition_by_name(in_mapping.destination)
        
        # Read input and write to output with transformation
        with open(in_table.full_path, 'r') as in_file, open(out_table.full_path, 'w', newline='') as out_file:
            reader = csv.DictReader(in_file)
            
            # Create writer with same field names
            fieldnames = reader.fieldnames
            writer = csv.DictWriter(out_file, fieldnames=fieldnames)
            writer.writeheader()
            
            # Transform each row and write to output
            for row in reader:
                # Simple transformation example - uppercase all values
                transformed_row = {k: v.upper() if isinstance(v, str) else v for k, v in row.items()}
                writer.writerow(transformed_row)
    else:
        # No input table, create sample output
        with open(out_table.full_path, 'w', newline='') as out_file:
            writer = csv.DictWriter(out_file, fieldnames=['id', 'data'])
            writer.writeheader()
            writer.writerow({'id': f'{i+1}', 'data': f'Sample data for {out_mapping.destination}'})
    
    # Write manifest
    ci.write_manifest(out_table)
```

## Processing input files

Similarly as tables, files and their manifest files are represented by the `keboola.component.dao.FileDefinition` object
and may be loaded using the convenience method `get_input_files_definitions()`. The result object contains all metadata
about the file, such as manifest file representations, system path and name.

The `get_input_files_definitions()` supports filter parameters to filter only files with a specific tag or retrieve only
the latest file of each. This is especially useful because the KBC input mapping will by default include all versions of
files matching specific tag. By default, the method returns only the latest file of each.

```python
from keboola.component import CommonInterface
import logging

# Initialize the interface
ci = CommonInterface()

# Get input files with specific tags (only latest versions)
input_files = ci.get_input_files_definitions(tags=['images', 'documents'], only_latest_files=True)

# Process each file
for file in input_files:
    print(f"Processing file: {file.name}")
    print(f"  - Full path: {file.full_path}")
    print(f"  - Tags: {file.tags}")
    
    # Example: Process image files
    if 'images' in file.tags:
        # Process image using appropriate library
        print(f"  - Processing image: {file.name}")
        # image = Image.open(file.full_path)
        # ... process image ...
    
    # Example: Process document files
    if 'documents' in file.tags:
        print(f"  - Processing document: {file.name}")
        # ... process document ...
```

### Grouping Files by Tags

When working with files it may be useful to retrieve them in a dictionary structure grouped by tag:

```python
from keboola.component import CommonInterface

# Initialize the interface
ci = CommonInterface()

# Group files by tag
files_by_tag = ci.get_input_file_definitions_grouped_by_tag_group(only_latest_files=True)

# Process files for each tag
for tag, files in files_by_tag.items():
    print(f"Processing tag group: {tag}")
    for file in files:
        print(f"  - File: {file.name}")
        # Process file based on its tag
```

### Creating Output Files

Similar to tables, you can create output files with appropriate manifests:

```python
from keboola.component import CommonInterface

# Initialize the interface
ci = CommonInterface()

# Create output file definition
output_file = ci.create_out_file_definition(
    name="results.json",
    tags=["processed", "results"],
    is_public=False,
    is_permanent=True
)

# Write content to the file
with open(output_file.full_path, 'w') as f:
    f.write('{"status": "success", "processed_records": 42}')

# Write manifest file
ci.write_manifest(output_file)
```

## Processing state files

[State files](https://developers.keboola.com/extend/common-interface/config-file/#state-file) allow your component to store and retrieve information between runs. This is especially useful for incremental processing or tracking the last processed data.

```python
from keboola.component import CommonInterface
from datetime import datetime
import json

# Initialize the interface
ci = CommonInterface()

# Load state from previous run
state = ci.get_state_file()

# Get the last processed timestamp (or use default if this is the first run)
last_updated = state.get("last_updated", "1970-01-01T00:00:00Z")
print(f"Last processed data up to: {last_updated}")

# Process data (only data newer than last_updated)
# In a real component, this would involve your business logic
processed_items = [
    {"id": 1, "timestamp": "2023-05-15T10:30:00Z"},
    {"id": 2, "timestamp": "2023-05-16T14:45:00Z"}
]

# Get the latest timestamp for the next run
if processed_items:
    # Sort items by timestamp to find the latest one
    processed_items.sort(key=lambda x: x["timestamp"])
    new_last_updated = processed_items[-1]["timestamp"]
else:
    # No new items, keep the previous timestamp
    new_last_updated = last_updated

# Store the new state for the next run
ci.write_state_file({
    "last_updated": new_last_updated,
    "processed_count": len(processed_items),
    "last_run": datetime.now().isoformat()
})

print(f"State updated, next run will process data from: {new_last_updated}")
```

State files can contain any serializable JSON structure, so you can store complex information:

```python
# More complex state example
state = {
    "last_run": datetime.now().isoformat(),
    "api_pagination": {
        "next_page_token": "abc123xyz",
        "page_size": 100,
        "total_pages_retrieved": 5
    },
    "processed_ids": [1001, 1002, 1003, 1004],
    "statistics": {
        "success_count": 1000,
        "error_count": 5,
        "skipped_count": 10
    }
}

ci.write_state_file(state)
```

## Logging

The library automatically initializes STDOUT or GELF logger based on the presence of the `KBC_LOGGER_PORT/HOST`
environment variable upon the `CommonInterface` initialization. To use the GELF logger just enable the logger for your
appplication in the Developer Portal. More info in
the [dedicated article](https://developers.keboola.com/extend/common-interface/logging/#examples).

Once it is enabled, you may just log your messages using the logging library:

```python
from keboola.component import CommonInterface
from datetime import datetime
import logging

# init the interface
ci = CommonInterface()

logging.info("Info message")
```

**TIP:** When the logger verbosity is set to `verbose` you may leverage `extra` fields to log the detailed message in
the detail of the log event by adding extra fields to you messages:

```python
logging.error(f'{error}. See log detail for full query. ',
              extra={"failed_query": json.dumps(query)})
```

You may also choose to override the settings by enabling the GELF or STDOUT explicitly and specifying the host/port
parameters:

```python
from keboola.component import CommonInterface
import os
import logging

# init the interface
ci = CommonInterface()
os.environ['KBC_LOGGER_ADDR'] = 'localhost'
os.environ['KBC_LOGGER_PORT'] = 12201
ci.set_gelf_logger(log_level=logging.INFO, transport_layer='UDP')

logging.info("Info message")
```

# ComponentBase

[Base class](https://keboola.github.io/python-component/base.html)
for general Python components. Base your components on this class for simpler debugging.

It performs following tasks by default:

- Initializes the CommonInterface.
- For easier debugging the data folder is picked up by default from `../data` path, relative to working directory when
  the `KBC_DATADIR` env variable is not specified.
- If `debug` parameter is present in the `config.json`, the default logger is set to verbose DEBUG mode.
- Executes sync actions -> `run` by default. See the sync actions section.

**Constructor arguments**:

- data_path_override: optional path to data folder that overrides the default behaviour
  (`KBC_DATADIR` environment variable). May be also specified by `-d` or `--data` commandline argument

Raises: `UserException` - on config validation errors.

**Example usage**:

```python
import csv
import logging
from datetime import datetime

from keboola.component.base import ComponentBase, sync_action
from keboola.component import UserException

# configuration variables
KEY_PRINT_HELLO = 'print_hello'

# list of mandatory parameters => if some is missing,
# component will fail with readable message on initialization.
REQUIRED_PARAMETERS = [KEY_PRINT_HELLO]
REQUIRED_IMAGE_PARS = []


class Component(ComponentBase):

    def run(self):
        '''
        Main execution code
        '''

        # ####### EXAMPLE TO REMOVE
        # check for missing configuration parameters
        self.validate_configuration_parameters(REQUIRED_PARAMETERS)
        self.validate_image_parameters(REQUIRED_IMAGE_PARS)

        params = self.configuration.parameters
        # Access parameters in data/config.json
        if params.get(KEY_PRINT_HELLO):
            logging.info("Hello World")

        # get last state data/in/state.json from previous run
        previous_state = self.get_state_file()
        logging.info(previous_state.get('some_state_parameter'))

        # Create output table (Tabledefinition - just metadata)
        table = self.create_out_table_definition('output.csv', incremental=True, primary_key=['timestamp'])

        # get file path of the table (data/out/tables/Features.csv)
        out_table_path = table.full_path
        logging.info(out_table_path)

        # DO whatever and save into out_table_path
        with open(table.full_path, mode='wt', encoding='utf-8', newline='') as out_file:
            writer = csv.DictWriter(out_file, fieldnames=['timestamp'])
            writer.writeheader()
            writer.writerow({"timestamp": datetime.now().isoformat()})

        # Save table manifest (output.csv.manifest) from the tabledefinition
        self.write_manifest(table)

        # Write new state - will be available next run
        self.write_state_file({"some_state_parameter": "value"})

        # ####### EXAMPLE TO REMOVE END

    # sync action that is executed when configuration.json "action":"testConnection" parameter is present.
    @sync_action('testConnection')
    def test_connection(self):
        connection = self.configuration.parameters.get('test_connection')
        if connection == "fail":
            raise UserException("failed")
        elif connection == "succeed":
            # this is ignored when run as sync action.
            logging.info("succeed")


"""
        Main entrypoint
"""
if __name__ == "__main__":
    try:
        comp = Component()
        # this triggers the run method by default and is controlled by the configuration.action paramter
        comp.execute_action()
    except UserException as exc:
        logging.exception(exc)
        exit(1)
    except Exception as exc:
        logging.exception(exc)
        exit(2)
```

## Table Schemas in ComponentBase

In cases of a static schemas of output/input tables, the schemas can be defined using a JSON Table Schema. For output
mapping these json schemas can be automatically turned into out table definitions.

### JSON Table Schema example file

```json
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
```

### Out table definition from schema example

The example below shows how a table definition can be created from a json schema using the ComponentBase. The schema is
located in the 'src/schemas' directory.

 ```python
import csv
from keboola.component.base import ComponentBase

DUMMY_PRODUCT_DATA = [{"id": "P0001",
                       "name": "juice"},
                      {"id": "P0002",
                       "name": "chocolate bar"},
                      {"id": "P0003",
                       "name": "Stylish Pants"},
                      ]


class Component(ComponentBase):

    def __init__(self):
        super().__init__()

    def run(self):
        product_schema = self.get_table_schema_by_name('product')
        product_table = self.create_out_table_definition_from_schema(product_schema)
        with open(product_table.full_path, 'w') as outfile:
            writer = csv.DictWriter(outfile, fieldnames=product_table.column_names)
            writer.writerows(DUMMY_PRODUCT_DATA)
        self.write_manifest(product_table)
 ```

# Sync Actions

[Sync actions](https://developers.keboola.com/extend/common-interface/actions/) provide a way to execute quick, synchronous tasks within a component. Unlike the default `run` action (which executes asynchronously as a background job), sync actions execute immediately and return results directly to the UI.

Common use cases for sync actions:
- Testing connections to external services
- Fetching dynamic dropdown options for UI configuration
- Validating user input
- Listing available resources (tables, schemas, etc.)

## Creating Sync Actions

To create a sync action, add a method to your component class and decorate it with `@sync_action('action_name')`. The framework handles all the details of proper response formatting and error handling.

```python
from keboola.component.base import ComponentBase, sync_action
from keboola.component import UserException

class Component(ComponentBase):
    def run(self):
        # Main component logic
        pass
        
    @sync_action('testConnection')
    def test_connection(self):
        """
        Tests database connection credentials
        """
        params = self.configuration.parameters
        connection = params.get('connection', {})
        
        # Validate connection parameters
        if not connection.get('host') or not connection.get('username'):
            raise UserException("Connection failed: Missing host or username")
            
        # If no exception is raised, the connection test is considered successful
        # The framework automatically returns {"status": "success"}
```

## Returning Data from Sync Actions

Sync actions can return data that is used by the UI, such as dropdown options:

```python
from keboola.component.base import ComponentBase, sync_action
from keboola.component.sync_actions import SelectElement

class Component(ComponentBase):
    @sync_action('listTables')
    def list_tables(self):
        """
        Returns list of available tables for configuration dropdown
        """
        # In a real scenario, you would fetch this from a database or API
        available_tables = [
            {"id": "customers", "name": "Customer Data"},
            {"id": "orders", "name": "Order History"},
            {"id": "products", "name": "Product Catalog"}
        ]
        
        # Return as list of SelectElement objects for UI dropdown
        return [
            SelectElement(value=table["id"], label=table["name"])
            for table in available_tables
        ]
```

## Validation Message Action

You can provide validation feedback to the UI:

```python
from keboola.component.base import ComponentBase, sync_action
from keboola.component.sync_actions import ValidationResult, MessageType

class Component(ComponentBase):
    @sync_action('validateConfiguration')
    def validate_config(self):
        """
        Validates the component configuration
        """
        params = self.configuration.parameters
        
        # Check configuration parameters
        if params.get('extraction_type') == 'incremental' and not params.get('incremental_key'):
            # Return warning message that will be displayed in UI
            return ValidationResult(
                "Incremental extraction requires specifying an incremental key column.",
                MessageType.WARNING
            )
            
        # Check for potential issues
        if params.get('row_limit') and int(params.get('row_limit')) > 1000000:
            # Return info message
            return ValidationResult(
                "Large row limit may cause performance issues.",
                MessageType.INFO
            )
            
        # Success with no message
        return None
```

#### No output

Some actions like test connection button expect only success / failure type of result with no return value.

```python
from keboola.component.base import ComponentBase, sync_action
from keboola.component import UserException
import logging


class Component(ComponentBase):

    def __init__(self):
        super().__init__()

    @sync_action('testConnection')
    def test_connection(self):
        # this is ignored when run as sync action.
        logging.info("Testing Connection")
        print("test print")
        params = self.configuration.parameters
        connection = params.get('test_connection')
        if connection == "fail":
            raise UserException("failed")
        elif connection == "succeed":
            # this is ignored when run as sync action.
            logging.info("succeed")
```

## License

MIT licensed, see [LICENSE](./LICENSE) file.
