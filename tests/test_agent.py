import json
import os
import unittest
from io import StringIO
from unittest.mock import patch

from keboola.component.base import ComponentBase, sync_action


class SimpleAgentComponent(ComponentBase):
    def run(self):
        pass

    def get_agent_context(self):
        return {'calculator': {'add': lambda a, b: a + b}}


class AgentDisabledComponent(ComponentBase):
    def run(self):
        pass

    def get_agent_schema(self):
        return None


class CustomSchemaComponent(ComponentBase):
    def run(self):
        pass

    def get_agent_schema(self):
        return {
            'component_name': 'test.component',
            'description': 'A test component',
            'methods': [
                {'name': 'do_thing', 'args': {}, 'returns': 'str'}
            ],
        }

    def get_agent_context(self):
        return {'greeting': 'hello'}


class ComponentWithSyncActions(ComponentBase):
    def run(self):
        pass

    @sync_action('testConnection')
    def test_connection(self):
        pass

    @sync_action('loadData')
    def load_data(self):
        """Loads data from source."""
        pass


class TestAgentSchema(unittest.TestCase):

    def setUp(self):
        path = os.path.join(os.path.dirname(os.path.realpath(__file__)),
                            'data_examples', 'data_agent_schema')
        os.environ['KBC_DATADIR'] = path

    def test_agent_schema_returns_auto_generated_schema(self):
        comp = SimpleAgentComponent()
        schema = comp.get_agent_schema()
        self.assertIsInstance(schema, dict)
        self.assertIn('modules', schema)
        self.assertIn('sync_actions', schema)
        self.assertIn('installed_packages', schema)
        self.assertIn('context_variables', schema)

    def test_agent_schema_contains_component_source(self):
        comp = SimpleAgentComponent()
        schema = comp.get_agent_schema()
        self.assertIn('component', schema['modules'])
        self.assertIn('SimpleAgentComponent', schema['modules']['component'])

    def test_agent_schema_disabled_returns_none(self):
        comp = AgentDisabledComponent()
        schema = comp.get_agent_schema()
        self.assertIsNone(schema)

    def test_agent_schema_custom_schema(self):
        comp = CustomSchemaComponent()
        schema = comp.get_agent_schema()
        self.assertEqual(schema['component_name'], 'test.component')
        self.assertEqual(schema['description'], 'A test component')
        self.assertIn('methods', schema)

    def test_agent_schema_includes_context_variables(self):
        comp = SimpleAgentComponent()
        schema = comp.get_agent_schema()
        self.assertIn('calculator', schema['context_variables'])
        self.assertEqual(schema['context_variables']['calculator']['type'], 'dict')

    def test_agent_schema_includes_installed_packages(self):
        comp = SimpleAgentComponent()
        schema = comp.get_agent_schema()
        self.assertIsInstance(schema['installed_packages'], list)

    @patch('sys.stdout', new_callable=StringIO)
    def test_agent_schema_sync_action_outputs_json(self, stdout):
        comp = SimpleAgentComponent()
        comp.execute_action()
        output = stdout.getvalue()
        parsed = json.loads(output)
        self.assertIn('modules', parsed)

    def test_agent_schema_sync_action_disabled_exits(self):
        with self.assertRaises(SystemExit):
            AgentDisabledComponent().execute_action()

    def test_agent_schema_lists_sync_actions(self):
        comp = ComponentWithSyncActions()
        schema = comp.get_agent_schema()
        action_names = [a['action'] for a in schema['sync_actions']]
        self.assertIn('testConnection', action_names)
        self.assertIn('loadData', action_names)
        self.assertNotIn('agentSchema', action_names)
        self.assertNotIn('agentCode', action_names)
        self.assertNotIn('run', action_names)

    def test_agent_schema_sync_action_has_doc(self):
        comp = ComponentWithSyncActions()
        schema = comp.get_agent_schema()
        load_data_action = next(a for a in schema['sync_actions'] if a['action'] == 'loadData')
        self.assertEqual(load_data_action['doc'], 'Loads data from source.')


class TestAgentCode(unittest.TestCase):

    def setUp(self):
        path = os.path.join(os.path.dirname(os.path.realpath(__file__)),
                            'data_examples', 'data_agent_code')
        os.environ['KBC_DATADIR'] = path

    @patch('sys.stdout', new_callable=StringIO)
    def test_agent_code_executes_simple_expression(self, stdout):
        comp = SimpleAgentComponent()
        comp.execute_action()
        output = stdout.getvalue()
        self.assertEqual(json.loads(output), {'result': 2})

    @patch('sys.stdout', new_callable=StringIO)
    def test_agent_code_has_comp_in_context(self, stdout):
        path = os.path.join(os.path.dirname(os.path.realpath(__file__)),
                            'data_examples', 'data_agent_code')
        os.environ['KBC_DATADIR'] = path

        config_path = os.path.join(path, 'config.json')
        with open(config_path, 'r') as f:
            config = json.load(f)
        original_code = config['parameters']['agent_code']

        config['parameters']['agent_code'] = 'result = type(comp).__name__'
        with open(config_path, 'w') as f:
            json.dump(config, f)

        try:
            comp = SimpleAgentComponent()
            comp.execute_action()
            output = stdout.getvalue()
            self.assertEqual(json.loads(output), {'result': 'SimpleAgentComponent'})
        finally:
            config['parameters']['agent_code'] = original_code
            with open(config_path, 'w') as f:
                json.dump(config, f)

    @patch('sys.stdout', new_callable=StringIO)
    def test_agent_code_has_custom_context(self, stdout):
        path = os.path.join(os.path.dirname(os.path.realpath(__file__)),
                            'data_examples', 'data_agent_code')
        os.environ['KBC_DATADIR'] = path

        config_path = os.path.join(path, 'config.json')
        with open(config_path, 'r') as f:
            config = json.load(f)
        original_code = config['parameters']['agent_code']

        config['parameters']['agent_code'] = "result = 'context_has_calculator' if 'add' in calculator else 'no'"
        with open(config_path, 'w') as f:
            json.dump(config, f)

        try:
            comp = SimpleAgentComponent()
            comp.execute_action()
            output = stdout.getvalue()
            self.assertEqual(json.loads(output), {'result': 'context_has_calculator'})
        finally:
            config['parameters']['agent_code'] = original_code
            with open(config_path, 'w') as f:
                json.dump(config, f)

    def test_agent_code_missing_code_param_exits(self):
        path = os.path.join(os.path.dirname(os.path.realpath(__file__)),
                            'data_examples', 'data_agent_code')
        os.environ['KBC_DATADIR'] = path

        config_path = os.path.join(path, 'config.json')
        with open(config_path, 'r') as f:
            config = json.load(f)
        original_code = config['parameters']['agent_code']

        config['parameters']['agent_code'] = ''
        with open(config_path, 'w') as f:
            json.dump(config, f)

        try:
            with self.assertRaises(SystemExit):
                SimpleAgentComponent().execute_action()
        finally:
            config['parameters']['agent_code'] = original_code
            with open(config_path, 'w') as f:
                json.dump(config, f)

    @patch('sys.stdout', new_callable=StringIO)
    def test_agent_code_returns_none_when_no_result(self, stdout):
        path = os.path.join(os.path.dirname(os.path.realpath(__file__)),
                            'data_examples', 'data_agent_code')
        os.environ['KBC_DATADIR'] = path

        config_path = os.path.join(path, 'config.json')
        with open(config_path, 'r') as f:
            config = json.load(f)
        original_code = config['parameters']['agent_code']

        config['parameters']['agent_code'] = 'x = 42'
        with open(config_path, 'w') as f:
            json.dump(config, f)

        try:
            comp = SimpleAgentComponent()
            comp.execute_action()
            output = stdout.getvalue()
            self.assertEqual(json.loads(output), {"status": "success"})
        finally:
            config['parameters']['agent_code'] = original_code
            with open(config_path, 'w') as f:
                json.dump(config, f)

    def test_agent_code_syntax_error_exits(self):
        path = os.path.join(os.path.dirname(os.path.realpath(__file__)),
                            'data_examples', 'data_agent_code')
        os.environ['KBC_DATADIR'] = path

        config_path = os.path.join(path, 'config.json')
        with open(config_path, 'r') as f:
            config = json.load(f)
        original_code = config['parameters']['agent_code']

        config['parameters']['agent_code'] = 'def broken('
        with open(config_path, 'w') as f:
            json.dump(config, f)

        try:
            with self.assertRaises(SystemExit):
                SimpleAgentComponent().execute_action()
        finally:
            config['parameters']['agent_code'] = original_code
            with open(config_path, 'w') as f:
                json.dump(config, f)


if __name__ == '__main__':
    unittest.main()
