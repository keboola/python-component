import os
import unittest

from keboola.component import CommonInterface, Configuration


class TestCommonInterface(unittest.TestCase):

    def setUp(self):
        path = os.path.join(os.path.dirname(os.path.realpath(__file__)),
                            'data_examples', 'data1')
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

    def test_missing_dir(self):
        os.environ["KBC_DATADIR"] = ""
        with self.assertRaisesRegex(
                ValueError,
                "Configuration file config.json not found"):
            CommonInterface()

    def test_get_data_dir(self):
        ci = CommonInterface()
        self.assertEqual(os.getenv('KBC_DATADIR', ''), ci.data_folder_path)


class TestConfiguration(unittest.TestCase):

    def setUp(self):
        path = os.path.join(os.path.dirname(os.path.realpath(__file__)),
                            'data_examples', 'data1')
        os.environ["KBC_DATADIR"] = path

    def test_missing_config(self):
        with self.assertRaisesRegex(
                ValueError,
                "Configuration file config.json not found"):
            Configuration('/non-existent/')

    def test_get_parameters(self):
        cfg = Configuration(os.environ["KBC_DATADIR"])
        params = cfg.parameters
        self.assertEqual({'fooBar': {'bar': 24, 'foo': 42}, 'baz': 'bazBar'},
                         params)
        self.assertEqual(params['fooBar']['foo'], 42)
        self.assertEqual(params['fooBar']['bar'], 24)

    def test_get_action(self):
        cfg = Configuration(os.environ["KBC_DATADIR"])

        self.assertEqual(cfg.action, 'test')

    def test_get_action_empty_config(self):
        cfg = Configuration(os.path.join(os.getenv('KBC_DATADIR', ''), '..',
                                         'data2'))
        self.assertEqual(cfg.action, '')

    def test_get_input_mappings(self):
        cfg = Configuration(os.environ["KBC_DATADIR"])
        tables = cfg.tables_input_mapping

        self.assertEqual(len(tables), 2)
        for table in tables:
            if table['destination'] == 'sample.csv':
                self.assertEqual(table['source'], 'in.c-main.test')
            else:
                self.assertEqual('in.c-main.test2', table['source'])

    def test_get_output_mapping(self):
        cfg = Configuration(os.environ["KBC_DATADIR"])
        tables = cfg.tables_output_mapping
        self.assertEqual(len(tables), 2)
        self.assertEqual(tables[0]['source'], 'results.csv')
        self.assertEqual(tables[1]['source'], 'results-new.csv')

    def test_empty_storage(self):
        cfg = Configuration(os.path.join(os.getenv('KBC_DATADIR', ''), '..',
                                         'data2'))
        self.assertEqual(cfg.tables_output_mapping, [])
        self.assertEqual(cfg.files_output_mapping, [])
        self.assertEqual(cfg.tables_input_mapping, [])
        self.assertEqual(cfg.files_input_mapping, [])
        self.assertEqual(cfg.parameters, {})

    def test_empty_params(self):
        cfg = Configuration(os.path.join(os.getenv('KBC_DATADIR', ''), '..',
                                         'data3'))
        self.assertEqual([], cfg.tables_output_mapping)
        self.assertEqual([], cfg.files_output_mapping)
        self.assertEqual({}, cfg.parameters)

    def test_get_authorization(self):
        cfg = Configuration(os.environ["KBC_DATADIR"])
        auth = cfg.oauth_credentials
        # self.assertEqual(auth['id'], "123456")
        self.assertEqual(auth["id"], "main")

    def test_get_oauthapi_data(self):
        cfg = Configuration(os.environ["KBC_DATADIR"])
        self.assertDictEqual(cfg.oauth_credentials.data, {"mykey": "myval"})

    def test_get_oauthapi_appsecret(self):
        cfg = Configuration(os.environ["KBC_DATADIR"])
        self.assertEqual(cfg.oauth_credentials.appSecret, "myappsecret")

    def test_get_oauthapi_appkey(self):
        cfg = Configuration(os.environ["KBC_DATADIR"])
        self.assertEqual(cfg.oauth_credentials.appKey, "myappkey")

    # def test_file_manifest(self):
    #     cfg = docker.Config()
    #     some_file = os.path.join(tempfile.mkdtemp('kbc-test') + 'someFile.txt')
    #     cfg.write_file_manifest(some_file, file_tags=['foo', 'bar'],
    #                             is_public=True, is_permanent=False,
    #                             notify=True)
    #     manifest_filename = some_file + '.manifest'
    #     with open(manifest_filename) as manifest_file:
    #         config = json.load(manifest_file)
    #     self.assertEqual(
    #         {'is_public': True, 'is_permanent': False, 'notify': True,
    #          'tags': ['foo', 'bar']},
    #         config
    #     )
    #     os.remove(manifest_filename)


if __name__ == '__main__':
    unittest.main()
