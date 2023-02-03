import os
import unittest

from keboola.component import UserException
from keboola.component.base import ComponentBase, sync_action


class MockComponent(ComponentBase):
    def run(self):
        return 'run_executed'


class MockComponentFail(ComponentBase):
    def run(self):
        raise UserException("Failed")


class TestCommonInterface(unittest.TestCase):

    def setUp(self):
        path = os.path.join(os.path.dirname(os.path.realpath(__file__)),
                            'data_examples', 'data1')
        os.environ["KBC_DATADIR"] = path

    def test_default_arguments_pass(self):
        MockComponent()

    def test_missing_config_parameters_fail(self):
        with self.assertRaises(UserException):
            MockComponent(required_parameters=['missing'])

    def test_missing_image_parameters_fail(self):
        with self.assertRaises(UserException):
            c = MockComponent(required_image_parameters=['missing'])
            c.execute_action()

    def test_missing_action_fail(self):
        path = os.path.join(os.path.dirname(os.path.realpath(__file__)),
                            'data_examples', 'data_custom_action')
        os.environ["KBC_DATADIR"] = path
        with self.assertRaises(AttributeError):
            MockComponent().execute_action()

    def test_run_action_passes(self):
        self.assertEqual(MockComponent().execute_action(), 'run_executed')

    def test_custom_action_passes(self):
        path = os.path.join(os.path.dirname(os.path.realpath(__file__)),
                            'data_examples', 'data_custom_action')
        os.environ["KBC_DATADIR"] = path

        class CustomActionComponent(ComponentBase):
            def run(self):
                pass

            @sync_action('custom_action')
            def test_action(self):
                return "run_executed"

        self.assertEqual(CustomActionComponent().execute_action(), 'run_executed')

    def test_run_action_fails_with_user_error(self):
        with self.assertRaises(UserException):
            MockComponentFail().execute_action()

    def test_system_action_name_fail(self):
        with self.assertRaises(ValueError):
            class ComponentInvalidActionName(ComponentBase):
                def run(self):
                    pass

                @sync_action('run')
                def test_action(self):
                    pass

            path = os.path.join(os.path.dirname(os.path.realpath(__file__)),
                                'data_examples', 'data_custom_action')
            os.environ["KBC_DATADIR"] = path

            ComponentInvalidActionName().execute_action()


if __name__ == '__main__':
    unittest.main()
