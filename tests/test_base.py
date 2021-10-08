import os
import unittest

from keboola.component.base import ComponentBase
from keboola.component import UserException


class MockComponent(ComponentBase):
    def run(self):
        pass


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
            MockComponent(required_image_parameters=['missing'])


if __name__ == '__main__':
    unittest.main()
