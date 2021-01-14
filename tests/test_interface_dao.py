import os
import tempfile
import unittest

from keboola.component.dao import *


class TestTableMetadata(unittest.TestCase):

    def test_full_column_datatypes_for_manifest_is_valid(self):
        column_metadata_full = {"col_1": [{
            "key": "KBC.datatype.basetype",
            "value": "NUMERIC"
        }, {
            "key": "KBC.datatype.nullable",
            "value": True
        }, {
            "key": "KBC.datatype.length",
            "value": "39,8"
        }, {
            "key": "KBC.datatype.default",
            "value": 0
        }
        ], "col_2": [{
            "key": "KBC.datatype.basetype",
            "value": "STRING"
        }, {
            "key": "KBC.datatype.nullable",
            "value": False
        }, {
            "key": "KBC.datatype.length",
            "value": "4000"
        }
        ]}
        tmetadata = TableMetadata()
        # col 1
        tmetadata.add_column_data_type("col_1", data_type='NUMERIC', nullable=True, length='39,8', default=0)
        # col 2
        tmetadata.add_column_data_type("col_2", data_type='STRING', nullable=False, length='4000')

        self.assertDictEqual(tmetadata.get_column_metadata_for_manifest(), column_metadata_full)

    def test_multi_column_datatypes_for_manifest_is_valid(self):
        column_metadata_full = {"col_1": [{
            "key": "KBC.datatype.basetype",
            "value": "NUMERIC"
        },
            {
                "key": "KBC.datatype.nullable",
                "value": False
            }
        ], "col_2": [{
            "key": "KBC.datatype.basetype",
            "value": "STRING"
        },
            {
                "key": "KBC.datatype.nullable",
                "value": False
            }
        ]}

        tmetadata = TableMetadata()
        tmetadata.add_column_data_types({"col_1": "NUMERIC", "col_2": "STRING"})

        self.assertDictEqual(tmetadata.get_column_metadata_for_manifest(), column_metadata_full)

    def test_datatype_accepts_enum_for_manifest_valid(self):
        column_metadata_full = {"col_1": [{
            "key": "KBC.datatype.basetype",
            "value": "NUMERIC"
        },
            {
                "key": "KBC.datatype.nullable",
                "value": False
            }
        ], "col_2": [{
            "key": "KBC.datatype.basetype",
            "value": "STRING"
        },
            {
                "key": "KBC.datatype.nullable",
                "value": False
            }
        ]}

        tmetadata = TableMetadata()
        tmetadata.add_column_data_types({"col_1": SupportedDataTypes.NUMERIC, "col_2": SupportedDataTypes.STRING})

        self.assertDictEqual(tmetadata.get_column_metadata_for_manifest(), column_metadata_full)

    def test_invalid_datatype_fails(self):
        tmetadata = TableMetadata()
        with self.assertRaises(ValueError) as ctx:
            tmetadata.add_column_data_type('col', 'invalid type')

    def test_table_description_metadata_for_manifest_is_valid(self):
        tmetadata = TableMetadata()

        table_metadata = [{
            "key": "KBC.description",
            "value": "Description of table"
        },
            {
                "key": "custom_key",
                "value": "custom_value"
            }
        ]
        tmetadata.add_table_description("Description of table")
        tmetadata.add_table_metadata("custom_key", "custom_value")
        self.assertEqual(tmetadata.get_table_metadata_for_manifest(), table_metadata)

    def test_build_from_manifest_valid(self):
        raw_manifest = {
            'destination': 'some-destination',
            'columns': ['foo', 'bar'],
            'primary_key': ['foo'],
            'incremental': True,
            'metadata': [{'key': 'bar', 'value': 'kochba'}],
            'column_metadata': {'bar': [{'key': 'foo', 'value': 'gogo'}]},
            'delete_where_column': 'lilly',
            'delete_where_values': ['a', 'b'],
            'delete_where_operator': 'eq'
        }

        table_metadata = TableMetadata(raw_manifest)

        expected_tmetadata = TableMetadata()

        expected_tmetadata.add_table_metadata("bar", "kochba")
        expected_tmetadata.add_column_metadata("bar", "foo", "gogo")

        self.assertEqual(table_metadata.column_metadata, expected_tmetadata.column_metadata)
        self.assertEqual(table_metadata.table_metadata, expected_tmetadata.table_metadata)


class TestTableDefinition(unittest.TestCase):

    def test_table_manifest_minimal(self):
        table_def = TableDefinition("testDef", "somepath", is_sliced=False,
                                    primary_key=['foo', 'bar']
                                    )

        self.assertEqual(
            {
                'primary_key': ['foo', 'bar'],
                'column_metadata': {},
                'metadata': []
            },
            table_def.get_manifest_dictionary()
        )

    def test_table_manifest_full(self):
        table_def = TableDefinition("testDef", "somepath", is_sliced=False,
                                    columns=['foo', 'bar'],
                                    destination='some-destination',
                                    primary_key=['foo'],
                                    incremental=True,
                                    delete_where={'column': 'lilly',
                                                  'values': ['a', 'b'],
                                                  'operator': 'eq'}
                                    )
        # add metadata
        table_def.table_metadata.add_column_metadata('bar', 'foo', 'gogo')
        table_def.table_metadata.add_table_metadata('bar', 'kochba')

        self.assertDictEqual(
            {
                'destination': 'some-destination',
                'columns': ['foo', 'bar'],
                'primary_key': ['foo'],
                'incremental': True,
                'metadata': [{'key': 'bar', 'value': 'kochba'}],
                'column_metadata': {'bar': [{'key': 'foo', 'value': 'gogo'}]},
                'delete_where_column': 'lilly',
                'delete_where_values': ['a', 'b'],
                'delete_where_operator': 'eq'
            },
            table_def.get_manifest_dictionary()
        )

    def test_build_from_table_manifest_metadata_equals(self):
        raw_manifest = {
            'destination': 'some-destination',
            'columns': ['foo', 'bar'],
            'primary_key': ['foo'],
            'incremental': True,
            'metadata': [{'key': 'bar', 'value': 'kochba'}],
            'column_metadata': {'bar': [{'key': 'foo', 'value': 'gogo'}]},
            'delete_where_column': 'lilly',
            'delete_where_values': ['a', 'b'],
            'delete_where_operator': 'eq'
        }

        manifest_file = os.path.join(tempfile.mkdtemp('kbc-test') + 'table.manifest')
        with open(manifest_file, 'w') as out_f:
            json.dump(raw_manifest, out_f)

        table_def = TableDefinition.build_from_manifest(manifest_file)

        expected_tmetadata = TableMetadata()

        expected_tmetadata.add_table_metadata("bar", "kochba")
        expected_tmetadata.add_column_metadata("bar", "foo", "gogo")

        self.assertEqual(table_def.table_metadata.column_metadata, expected_tmetadata.column_metadata)
        self.assertEqual(table_def.table_metadata.table_metadata, expected_tmetadata.table_metadata)

        os.remove(manifest_file)

    def test_build_from_manifest_matching_table_valid_attributes(self):
        sample_path = os.path.join(os.path.dirname(os.path.realpath(__file__)),
                                   'data_examples', 'data1', 'in', 'tables')

        table_def = TableDefinition.build_from_manifest(os.path.join(sample_path, 'sample.csv.manifest'))

        expected_table_def = TableDefinition(name='sample.csv',
                                             full_path=os.path.join(sample_path, 'sample.csv'),
                                             is_sliced=False
                                             )

        self.assertEqual(expected_table_def.full_path, table_def.full_path)
        self.assertEqual(expected_table_def.name, table_def.name)
        self.assertEqual(expected_table_def.is_sliced, table_def.is_sliced)

    def test_build_from_manifest_orphaned_table_valid_attributes(self):
        sample_path = os.path.join(os.path.dirname(os.path.realpath(__file__)),
                                   'data_examples', 'data1', 'in', 'tables')

        table_def = TableDefinition.build_from_manifest(os.path.join(sample_path, 'orphaned.csv.manifest'))

        expected_table_def = TableDefinition(name='orphaned.csv',
                                             full_path=os.path.join(sample_path, 'orphaned.csv'),
                                             is_sliced=False
                                             )

        self.assertEqual(expected_table_def.full_path, table_def.full_path)
        self.assertEqual(expected_table_def.name, table_def.name)
        self.assertEqual(expected_table_def.is_sliced, table_def.is_sliced)
        self.assertEqual(expected_table_def.get_manifest_dictionary(), table_def.get_manifest_dictionary())

    def test_build_from_manifest_sliced_table_valid_attributes(self):
        sample_path = os.path.join(os.path.dirname(os.path.realpath(__file__)),
                                   'data_examples', 'data1', 'in', 'tables')

        table_def = TableDefinition.build_from_manifest(os.path.join(sample_path, 'sliced.csv.manifest'))

        expected_table_def = TableDefinition(name='sliced.csv',
                                             full_path=os.path.join(sample_path, 'sliced.csv'),
                                             is_sliced=True
                                             )

        self.assertEqual(expected_table_def.full_path, table_def.full_path)
        self.assertEqual(expected_table_def.name, table_def.name)
        self.assertEqual(expected_table_def.is_sliced, table_def.is_sliced)

    def test_build_from_manifest_orphaned_manifest_valid_attributes(self):
        sample_path = os.path.join(os.path.dirname(os.path.realpath(__file__)),
                                   'data_examples', 'data1', 'in', 'tables')

        table_def = TableDefinition.build_from_manifest(os.path.join(sample_path, 'orphaned_manifest.csv.manifest'))

        expected_table_def = TableDefinition(name='orphaned_manifest.csv',
                                             full_path=None,
                                             is_sliced=False
                                             )

        self.assertEqual(expected_table_def.full_path, table_def.full_path)
        self.assertEqual(expected_table_def.name, table_def.name)
        self.assertEqual(expected_table_def.is_sliced, table_def.is_sliced)

    def test_table_manifest_error_destination(self):
        with self.assertRaises(TypeError):
            TableDefinition("testDef", "somepath", is_sliced=False, destination=['foo', 'bar'])

    def test_table_manifest_error_primary_key(self):
        with self.assertRaises(TypeError):
            TableDefinition("testDef", "somepath", is_sliced=False, primary_key="column")

    def test_table_manifest_error_columns(self):
        with self.assertRaises(TypeError):
            TableDefinition("testDef", "somepath", is_sliced=False, columns="column")

    def test_table_manifest_error_column_delete_1(self):
        with self.assertRaises(ValueError):
            TableDefinition("testDef", "somepath", is_sliced=False, delete_where={"a": "b"})

    def test_table_manifest_error_column_delete_2(self):
        with self.assertRaises(TypeError):
            TableDefinition("testDef", "somepath", is_sliced=False, delete_where={"column": "a",
                                                                                  "values": "b"})

    def test_table_manifest_error_column_delete_3(self):
        with self.assertRaises(TypeError):
            TableDefinition("testDef", "somepath", is_sliced=False, delete_where={"column": "a",
                                                                                  "values": "b",
                                                                                  "operator": "c"})
