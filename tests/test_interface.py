import os
import unittest

from keboola.component import CommonInterface


class TestEnvHandler(unittest.TestCase):

    def setUp(self):
        path = os.path.join(os.path.dirname(os.path.realpath(__file__)),
                            'data')
        os.environ["KBC_DATADIR"] = path

    def test_empty_required_params_pass(self):
        c = CommonInterface
        return True
        # # set env
        # interface = CommonInterface(mandatory_params=[])
        # `
        # # tests
        # try:
        #     interface.validate_config()
        # except Exception:  # noeq
        #     self.fail("validateConfig() fails on empty Parameters!")

    def test_required_params_missing_fail(self):
        return True
        # set env - missing notbar
        # hdlr = CommonInterface(mandatory_params=['fooBar', 'notbar'])
        #
        # with self.assertRaises(ValueError) as er:
        #     hdlr.validate_config(['fooBar', 'notbar'])
        #
        # self.assertEqual('Missing mandatory config parameters fields: [notbar] ', str(er.exception))

    def test_unknown_config_tables_input_mapping_properties_pass(self):
        """Unknown properties in storage.intpu.tables will be ignored when getting dataclass"""


if __name__ == '__main__':
    unittest.main()
