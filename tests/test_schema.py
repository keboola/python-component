import os
import unittest

from keboola.component.base import ComponentBase


class MockComponent(ComponentBase):
    def run(self):
        return 'run_executed'


class TestCommonInterface(unittest.TestCase):

    def setUp(self):
        path = os.path.join(os.path.dirname(os.path.realpath(__file__)), 'data_examples', 'data1')
        os.environ["KBC_DATADIR"] = path
        # to simulate kbc run
        os.environ["KBC_STACKID"] = 'test'

    def test_create_out_table_definition_from_schema_name(self):
        schema_path = os.path.join(os.path.dirname(os.path.realpath(__file__)), 'schema_examples', 'schemas')
        comp = MockComponent(schema_path_override=schema_path)
        order_schema = comp.get_table_schema_by_name(schema_name="order")
        order_table_definition_from_schema = comp.create_out_table_definition_from_schema(order_schema)
        self.assertEqual("order.csv", order_table_definition_from_schema.name)
        self.assertEqual(["id", "product_id", "quantity"], order_table_definition_from_schema.columns)
        self.assertEqual(["id"], order_table_definition_from_schema.primary_key)

    def test_created_manifest_against_schema(self):
        schema_path = os.path.join(os.path.dirname(os.path.realpath(__file__)), 'schema_examples', 'schemas')
        comp = MockComponent(schema_path_override=schema_path)
        order_schema = comp.get_table_schema_by_name(schema_name="order")
        order_table_definition_from_schema = comp.create_out_table_definition_from_schema(order_schema)
        manifest_dict = order_table_definition_from_schema.get_manifest_dictionary(legacy_manifest=True)
        expected_manifest = {'primary_key': ['id'], 'columns': ['id', 'product_id', 'quantity'], 'enclosure': '"',
                             'delimiter': ',',
                             'write_always': False,
                             'metadata': [{'key': 'KBC.description', 'value': 'this table holds data on orders'}],
                             'column_metadata': {'id': [{'key': 'KBC.description', 'value': 'ID of the order'},
                                                        {'key': 'KBC.datatype.basetype', 'value': 'STRING'},
                                                        {'key': 'KBC.datatype.nullable', 'value': False}],
                                                 'product_id': [
                                                     {'key': 'KBC.description', 'value': 'Id of the product in order'},
                                                     {'key': 'KBC.datatype.basetype', 'value': 'NUMERIC'},
                                                     {'key': 'KBC.datatype.nullable', 'value': False}],
                                                 'quantity': [
                                                     {'key': 'KBC.description',
                                                      'value': 'Quantity of the product in order'},
                                                     {'key': 'KBC.datatype.basetype', 'value': 'STRING'},
                                                     {'key': 'KBC.datatype.nullable', 'value': False}]}}
        self.assertEqual(expected_manifest, manifest_dict)

    def test_created_manifest_against_schema_new_manifest(self):
        os.environ['KBC_DATA_TYPE_SUPPORT'] = "authoritative"

        schema_path = os.path.join(os.path.dirname(os.path.realpath(__file__)), 'schema_examples', 'schemas')
        comp = MockComponent(schema_path_override=schema_path)
        order_schema = comp.get_table_schema_by_name(schema_name="order")
        order_table_definition_from_schema = comp.create_out_table_definition_from_schema(order_schema)
        manifest_dict = order_table_definition_from_schema.get_manifest_dictionary(legacy_manifest=False)

        expected_manifest = {'delimiter': ',',
                             'enclosure': '"',
                             'has_header': False,
                             'manifest_type': 'out',
                             'schema': [{'data_type': {'base': {'type': 'STRING'}},
                                         'description': 'ID of the order',
                                         'name': 'id',
                                         'primary_key': True},
                                        {'data_type': {'base': {'type': 'NUMERIC'}},
                                         'description': 'Id of the product in order',
                                         'name': 'product_id'},
                                        {'data_type': {'base': {'type': 'STRING'}},
                                         'description': 'Quantity of the product in order',
                                         'name': 'quantity'}],
                             'write_always': False}
        del os.environ['KBC_DATA_TYPE_SUPPORT']
        self.assertEqual(expected_manifest, manifest_dict)

    def test_invalid_column_schema_raises_key_error(self):
        with self.assertRaises(KeyError):
            schema_path = os.path.join(os.path.dirname(os.path.realpath(__file__)), 'schema_examples', 'faulty-schemas')
            comp = MockComponent(schema_path_override=schema_path)
            table_schema = comp.get_table_schema_by_name(schema_name="invalid_column_schema")
            comp.create_out_table_definition_from_schema(table_schema)

    def test_invalid_schema_raises_key_error(self):
        with self.assertRaises(KeyError):
            schema_path = os.path.join(os.path.dirname(os.path.realpath(__file__)), 'schema_examples', 'faulty-schemas')
            comp = MockComponent(schema_path_override=schema_path)
            table_schema = comp.get_table_schema_by_name(schema_name="invalid_table_schema")
            comp.create_out_table_definition_from_schema(table_schema)

    def test_missing_schema_raises_key_error(self):
        with self.assertRaises(FileNotFoundError):
            schema_path = os.path.join(os.path.dirname(os.path.realpath(__file__)), 'schema_examples', 'faulty-schemas')
            comp = MockComponent(schema_path_override=schema_path)
            table_schema = comp.get_table_schema_by_name(schema_name="missing")
            comp.create_out_table_definition_from_schema(table_schema)

    def test_invalid_schema_path_raises_key_error(self):
        with self.assertRaises(FileNotFoundError):
            schema_path = os.path.join(os.path.dirname(os.path.realpath(__file__)), 'schema_examples', 'missing')
            comp = MockComponent(schema_path_override=schema_path)
            table_schema = comp.get_table_schema_by_name(schema_name="missing")
            comp.create_out_table_definition_from_schema(table_schema)

    def test_invalid_base_type_raises_key_error(self):
        with self.assertRaises(ValueError):
            schema_path = os.path.join(os.path.dirname(os.path.realpath(__file__)), 'schema_examples', 'faulty-schemas')
            comp = MockComponent(schema_path_override=schema_path)
            table_schema = comp.get_table_schema_by_name(schema_name="invalid_base_type")
            comp.create_out_table_definition_from_schema(table_schema)


if __name__ == '__main__':
    unittest.main()
