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
