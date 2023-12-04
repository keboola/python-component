import json
import os
import unittest

from keboola.component import CommonInterface, Configuration


class TestCommonInterface(unittest.TestCase):

    def setUp(self):
        path = os.path.join(os.path.dirname(os.path.realpath(__file__)),
                            'data_examples', 'data1')
        os.environ["KBC_DATADIR"] = path

    def test_all_env_variables_initialized(self):
        # set all variables
        os.environ['KBC_RUNID'] = 'KBC_RUNID'
        os.environ['KBC_PROJECTID'] = 'KBC_PROJECTID'
        os.environ['KBC_STACKID'] = 'KBC_STACKID'
        os.environ['KBC_CONFIGID'] = 'KBC_CONFIGID'
        os.environ['KBC_COMPONENTID'] = 'KBC_COMPONENTID'
        os.environ['KBC_PROJECTNAME'] = 'KBC_PROJECTNAME'
        os.environ['KBC_TOKENID'] = 'KBC_TOKENID'
        os.environ['KBC_TOKENDESC'] = 'KBC_TOKENDESC'
        os.environ['KBC_TOKEN'] = 'KBC_TOKEN'
        os.environ['KBC_URL'] = 'KBC_URL'
        os.environ['KBC_LOGGER_ADDR'] = 'KBC_LOGGER_ADDR'
        os.environ['KBC_LOGGER_PORT'] = 'KBC_LOGGER_PORT'

        ci = CommonInterface()
        self.assertEqual(ci.environment_variables.data_dir, os.environ["KBC_DATADIR"])
        self.assertEqual(ci.environment_variables.run_id, 'KBC_RUNID')
        self.assertEqual(ci.environment_variables.project_id, 'KBC_PROJECTID')
        self.assertEqual(ci.environment_variables.stack_id, 'KBC_STACKID')
        self.assertEqual(ci.environment_variables.config_id, 'KBC_CONFIGID')
        self.assertEqual(ci.environment_variables.component_id, 'KBC_COMPONENTID')
        self.assertEqual(ci.environment_variables.project_name, 'KBC_PROJECTNAME')
        self.assertEqual(ci.environment_variables.token_id, 'KBC_TOKENID')
        self.assertEqual(ci.environment_variables.token_desc, 'KBC_TOKENDESC')
        self.assertEqual(ci.environment_variables.token, 'KBC_TOKEN')
        self.assertEqual(ci.environment_variables.url, 'KBC_URL')
        self.assertEqual(ci.environment_variables.logger_addr, 'KBC_LOGGER_ADDR')
        self.assertEqual(ci.environment_variables.logger_port, 'KBC_LOGGER_PORT')

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
        os.environ["KBC_DATADIR"] = "asdf"
        with self.assertRaisesRegex(
                ValueError,
                "The data directory does not exist"):
            CommonInterface()

    # ########## PROPERTIES

    def test_missing_config(self):
        os.environ["KBC_DATADIR"] = os.path.join(os.path.dirname(os.path.realpath(__file__)),
                                                 'data_examples')
        with self.assertRaisesRegex(
                ValueError,
                "Configuration file config.json not found"):
            ci = CommonInterface()
            c = ci.configuration

    def test_get_data_dir(self):
        ci = CommonInterface()
        self.assertEqual(os.getenv('KBC_DATADIR', ''), ci.data_folder_path)

    def test_get_tables_out_dir(self):
        ci = CommonInterface()
        tables_out = os.path.join(os.getenv('KBC_DATADIR', ''), 'out', 'tables')
        self.assertEqual(tables_out, ci.tables_out_path)

    def test_get_tables_in_dir(self):
        ci = CommonInterface()
        tables_out = os.path.join(os.getenv('KBC_DATADIR', ''), 'in', 'files')
        self.assertEqual(tables_out, ci.files_in_path)

    def test_get_files_out_dir(self):
        ci = CommonInterface()
        tables_out = os.path.join(os.getenv('KBC_DATADIR', ''), 'out', 'files')
        self.assertEqual(tables_out, ci.files_out_path)

    def test_get_files_in_dir(self):
        ci = CommonInterface()
        tables_out = os.path.join(os.getenv('KBC_DATADIR', ''), 'in', 'tables')
        self.assertEqual(tables_out, ci.tables_in_path)

    def test_legacy_queue(self):
        os.environ['KBC_PROJECT_FEATURE_GATES'] = ''
        ci = CommonInterface()
        # with no env default to v2
        self.assertEqual(False, ci.is_legacy_queue)

        # otherwise check for queuev2
        os.environ['KBC_PROJECT_FEATURE_GATES'] = 'queuev2;someoterfeature'
        self.assertEqual(False, ci.is_legacy_queue)

        # If feature gates exists but doesn't contain queuev2 it's old queue
        os.environ['KBC_PROJECT_FEATURE_GATES'] = 'feature1;someoterfeature'
        self.assertEqual(True, ci.is_legacy_queue)

    def test_create_and_write_table_manifest_deprecated(self):
        ci = CommonInterface()
        # create table def
        out_table = ci.create_out_table_definition('some-table.csv',
                                                   columns=['foo', 'bar'],
                                                   destination='some-destination',
                                                   primary_key=['foo'],
                                                   incremental=True,
                                                   delete_where={'column': 'lilly',
                                                                 'values': ['a', 'b'],
                                                                 'operator': 'eq'}
                                                   )
        out_table.table_metadata.add_table_metadata('bar', 'kochba')
        out_table.table_metadata.add_column_metadata('bar', 'foo', 'gogo')

        # write
        ci.write_tabledef_manifest(out_table)
        manifest_filename = out_table.full_path + '.manifest'
        with open(manifest_filename) as manifest_file:
            config = json.load(manifest_file)
        self.assertEqual(
            {
                'destination': 'some-destination',
                'columns': ['foo', 'bar'],
                'primary_key': ['foo'],
                'incremental': True,
                'delimiter': ',',
                'enclosure': '"',
                'metadata': [{'key': 'bar', 'value': 'kochba'}],
                'column_metadata': {'bar': [{'key': 'foo', 'value': 'gogo'}]},
                'delete_where_column': 'lilly',
                'delete_where_values': ['a', 'b'],
                'delete_where_operator': 'eq',
                'write_always': False
            },
            config
        )
        os.remove(manifest_filename)

    def test_create_and_write_table_manifest(self):
        ci = CommonInterface()
        # create table def
        out_table = ci.create_out_table_definition('some-table.csv',
                                                   columns=['foo', 'bar'],
                                                   destination='some-destination',
                                                   primary_key=['foo'],
                                                   incremental=True,
                                                   delete_where={'column': 'lilly',
                                                                 'values': ['a', 'b'],
                                                                 'operator': 'eq'},
                                                   write_always=True
                                                   )
        out_table.table_metadata.add_table_metadata('bar', 'kochba')
        out_table.table_metadata.add_column_metadata('bar', 'foo', 'gogo')

        # write
        ci.write_manifest(out_table)
        manifest_filename = out_table.full_path + '.manifest'
        with open(manifest_filename) as manifest_file:
            config = json.load(manifest_file)
        self.assertEqual(
            {
                'destination': 'some-destination',
                'columns': ['foo', 'bar'],
                'primary_key': ['foo'],
                'incremental': True,
                'write_always': True,
                'delimiter': ',',
                'enclosure': '"',
                'metadata': [{'key': 'bar', 'value': 'kochba'}],
                'column_metadata': {'bar': [{'key': 'foo', 'value': 'gogo'}]},
                'delete_where_column': 'lilly',
                'delete_where_values': ['a', 'b'],
                'delete_where_operator': 'eq'
            },
            config
        )
        os.remove(manifest_filename)

    def test_create_and_write_table_manifest_old_queue(self):
        # If feature gates exists but doesn't contain queuev2 it's old queue
        os.environ['KBC_PROJECT_FEATURE_GATES'] = 'feature1;someoterfeature'

        ci = CommonInterface()
        # create table def
        out_table = ci.create_out_table_definition('some-table.csv',
                                                   columns=['foo', 'bar'],
                                                   destination='some-destination',
                                                   primary_key=['foo'],
                                                   incremental=True,
                                                   # the write_always will then not be present in the manifest even if set
                                                   write_always=True,
                                                   delete_where={'column': 'lilly',
                                                                 'values': ['a', 'b'],
                                                                 'operator': 'eq'}
                                                   )
        out_table.table_metadata.add_table_metadata('bar', 'kochba')
        out_table.table_metadata.add_column_metadata('bar', 'foo', 'gogo')

        # write
        ci.write_manifest(out_table)
        manifest_filename = out_table.full_path + '.manifest'
        with open(manifest_filename) as manifest_file:
            config = json.load(manifest_file)
        self.assertEqual(
            {
                'destination': 'some-destination',
                'columns': ['foo', 'bar'],
                'primary_key': ['foo'],
                'incremental': True,
                'delimiter': ',',
                'enclosure': '"',
                'metadata': [{'key': 'bar', 'value': 'kochba'}],
                'column_metadata': {'bar': [{'key': 'foo', 'value': 'gogo'}]},
                'delete_where_column': 'lilly',
                'delete_where_values': ['a', 'b'],
                'delete_where_operator': 'eq'
            },
            config
        )
        os.remove(manifest_filename)

    # #### DATA FOLDER MANIPULATION
    def test_create_and_write_table_manifest_multi_deprecated(self):
        ci = CommonInterface()
        # create table def
        out_table = ci.create_out_table_definition('some-table.csv',
                                                   columns=['foo', 'bar'],
                                                   destination='some-destination',
                                                   primary_key=['foo'],
                                                   incremental=True,
                                                   delete_where={'column': 'lilly',
                                                                 'values': ['a', 'b'],
                                                                 'operator': 'eq'}
                                                   )
        out_table.table_metadata.add_table_metadata('bar', 'kochba')
        out_table.table_metadata.add_column_metadata('bar', 'foo', 'gogo')

        # write
        ci.write_tabledef_manifests([out_table])
        manifest_filename = out_table.full_path + '.manifest'
        with open(manifest_filename) as manifest_file:
            config = json.load(manifest_file)
        self.assertEqual(
            {
                'destination': 'some-destination',
                'columns': ['foo', 'bar'],
                'primary_key': ['foo'],
                'incremental': True,
                'metadata': [{'key': 'bar', 'value': 'kochba'}],
                'delimiter': ',',
                'enclosure': '"',
                'column_metadata': {'bar': [{'key': 'foo', 'value': 'gogo'}]},
                'delete_where_column': 'lilly',
                'delete_where_values': ['a', 'b'],
                'delete_where_operator': 'eq',
                'write_always': False
            },
            config
        )
        os.remove(manifest_filename)

    def test_create_and_write_table_manifest_multi(self):
        ci = CommonInterface()
        # create table def
        out_table = ci.create_out_table_definition('some-table.csv',
                                                   columns=['foo', 'bar'],
                                                   destination='some-destination',
                                                   primary_key=['foo'],
                                                   incremental=True,
                                                   delete_where={'column': 'lilly',
                                                                 'values': ['a', 'b'],
                                                                 'operator': 'eq'}
                                                   )
        out_table.table_metadata.add_table_metadata('bar', 'kochba')
        out_table.table_metadata.add_column_metadata('bar', 'foo', 'gogo')

        # write
        ci.write_manifests([out_table])
        manifest_filename = out_table.full_path + '.manifest'
        with open(manifest_filename) as manifest_file:
            config = json.load(manifest_file)
        self.assertEqual(
            {
                'destination': 'some-destination',
                'columns': ['foo', 'bar'],
                'primary_key': ['foo'],
                'incremental': True,
                'metadata': [{'key': 'bar', 'value': 'kochba'}],
                'delimiter': ',',
                'enclosure': '"',
                'column_metadata': {'bar': [{'key': 'foo', 'value': 'gogo'}]},
                'delete_where_column': 'lilly',
                'delete_where_values': ['a', 'b'],
                'delete_where_operator': 'eq',
                'write_always': False
            },
            config
        )
        os.remove(manifest_filename)

    def test_get_input_tables_definition(self):
        ci = CommonInterface()

        tables = ci.get_input_tables_definitions()

        self.assertEqual(len(tables), 4)
        for table in tables:
            if table.name == 'sample.csv':
                self.assertEqual(table.columns, [
                    "x",
                    "Sales",
                    "CompPrice",
                    "Income",
                    "Advertising",
                    "Population",
                    "Price",
                    "ShelveLoc",
                    "Age",
                    "Education",
                    "Urban",
                    "US",
                    "High"
                ])
                self.assertEqual(table.rows_count, 400)
                self.assertEqual(table.data_size_bytes, 81920)
            elif table.name == 'fooBar':
                self.assertEqual(table.id, 'in.c-main.test2')
                self.assertEqual(table.full_path, os.path.join(ci.tables_in_path, 'fooBar'))
                self.assertEqual(table.name, 'fooBar')

    def test_get_input_tables_definition_orphaned_manifest(self):
        ci = CommonInterface()

        tables = ci.get_input_tables_definitions(orphaned_manifests=True)

        self.assertEqual(len(tables), 5)
        for table in tables:
            if table.name == 'sample.csv':
                self.assertEqual(table.columns, [
                    "x",
                    "Sales",
                    "CompPrice",
                    "Income",
                    "Advertising",
                    "Population",
                    "Price",
                    "ShelveLoc",
                    "Age",
                    "Education",
                    "Urban",
                    "US",
                    "High"
                ])
                self.assertEqual(table.rows_count, 400)
                self.assertEqual(table.data_size_bytes, 81920)
            elif table.name == 'fooBar':
                self.assertEqual(table.id, 'in.c-main.test2')
                self.assertEqual(table.full_path, os.path.join(ci.tables_in_path, 'fooBar'))
                self.assertEqual(table.name, 'fooBar')

    def test_state_file_initialized(self):
        ci = CommonInterface()
        state = ci.get_state_file()
        self.assertEqual(state['test_state'], 1234)

    def test_state_file_created(self):
        ci = CommonInterface()
        # write
        ci.write_state_file({"some_state": 1234})

        # load
        state_filename = os.path.join(ci.data_folder_path, 'out', 'state.json')
        with open(state_filename) as state_file:
            state = json.load(state_file)

        self.assertEqual(
            {"some_state": 1234},
            state
        )

        # cleanup
        os.remove(state_filename)

    def test_get_input_table_by_name_fails_on_nonexistent(self):
        ci = CommonInterface()
        with self.assertRaises(ValueError):
            ci.get_input_table_definition_by_name('nonexistent.csv')

    def test_get_input_table_by_name_existing_passes(self):
        ci = CommonInterface()
        in_table = ci.get_input_table_definition_by_name('fooBar')
        self.assertEqual(in_table.id, 'in.c-main.test2')
        self.assertEqual(in_table.full_path, os.path.join(ci.tables_in_path, 'fooBar'))
        self.assertEqual(in_table.name, 'fooBar')

    # Files

    def test_create_and_write_file_manifest_deprecated(self):
        ci = CommonInterface()
        # create table def
        out_file = ci.create_out_file_definition('some-file.jpg',
                                                 is_permanent=True,
                                                 is_encrypted=True,
                                                 is_public=True,
                                                 tags=['foo', 'bar'],
                                                 notify=True
                                                 )

        # write
        ci.write_filedef_manifest(out_file)
        manifest_filename = out_file.full_path + '.manifest'
        with open(manifest_filename) as manifest_file:
            config = json.load(manifest_file)
        self.assertEqual(
            {'tags': ['foo', 'bar'],
             'is_public': True,
             'is_permanent': True,
             'is_encrypted': True,
             'notify': True},
            config
        )
        os.remove(manifest_filename)

    def test_create_and_write_file_manifest(self):
        ci = CommonInterface()
        # create table def
        out_file = ci.create_out_file_definition('some-file.jpg',
                                                 is_permanent=True,
                                                 is_encrypted=True,
                                                 is_public=True,
                                                 tags=['foo', 'bar'],
                                                 notify=True
                                                 )

        # write
        ci.write_manifest(out_file)
        manifest_filename = out_file.full_path + '.manifest'
        with open(manifest_filename) as manifest_file:
            config = json.load(manifest_file)
        self.assertEqual(
            {'tags': ['foo', 'bar'],
             'is_public': True,
             'is_permanent': True,
             'is_encrypted': True,
             'notify': True},
            config
        )
        os.remove(manifest_filename)

    def test_get_input_files_definition_latest(self):
        ci = CommonInterface()

        files = ci.get_input_files_definitions()

        self.assertEqual(len(files), 5)
        for file in files:
            if file.name == 'duty_calls.png':
                self.assertEqual(file.id, '151971455')

    def test_get_input_files_definition_by_tag(self):
        ci = CommonInterface()

        files = ci.get_input_files_definitions(tags=['dilbert'])

        self.assertEqual(len(files), 3)
        for file in files:
            if file.name == '21702.strip.print.gif':
                self.assertEqual(file.tags, [
                    "dilbert"
                ])
                self.assertEqual(file.max_age_days, 180)
                self.assertEqual(file.size_bytes, 4931)

    def test_get_input_files_definition_by_tag_w_system(self):
        ci = CommonInterface(os.path.join(os.path.dirname(os.path.realpath(__file__)),
                                          'data_examples', 'data_system_tags'))

        files = ci.get_input_files_definitions(tags=['dilbert'])

        self.assertEqual(len(files), 3)
        for file in files:
            if file.name == '21702.strip.print.gif':
                self.assertEqual(file.tags, [
                    "dilbert",
                    "componentId: 1234",
                    "configurationId: 12345",
                    "configurationRowId: 12345",
                    "runId: 22123",
                    "branchId: 312321"
                ])
                self.assertEqual(file.max_age_days, 180)
                self.assertEqual(file.size_bytes, 4931)

    def test_get_input_files_definition_tag_group_w_system(self):
        ci = CommonInterface(os.path.join(os.path.dirname(os.path.realpath(__file__)),
                                          'data_examples', 'data_system_tags'))

        files = ci.get_input_file_definitions_grouped_by_tag_group(only_latest_files=False)

        self.assertEqual(len(files), 2)
        self.assertEqual(len(files["bar;foo"]), 3)
        for file in files["bar;foo"]:
            if file.name == 'compiler_complaint.png':
                self.assertEqual(file.tags, [
                    "foo",
                    "bar",
                    "componentId: 1234",
                    "configurationId: 12345",
                    "configurationRowId: 12345",
                    "runId: 22123",
                    "branchId: 312321"
                ])

    def test_get_input_files_definition_nofilter(self):
        ci = CommonInterface()

        files = ci.get_input_files_definitions(only_latest_files=False)

        self.assertEqual(len(files), 6)
        for file in files:
            if file.name == 'duty_calls':
                self.assertEqual(file.tags, [
                    "xkcd"
                ])
                self.assertEqual(file.max_age_days, 180)
                self.assertEqual(file.size_bytes, 30027)

    def test_get_input_files_definition_no_manifest_passes(self):
        ci = CommonInterface(os.path.join(os.path.dirname(os.path.realpath(__file__)),
                                          'data_examples', 'data2'))

        files = ci.get_input_files_definitions(only_latest_files=True)

        self.assertEqual(len(files), 1)
        for file in files:
            self.assertEqual(file.max_age_days, 0)
            self.assertEqual(file.size_bytes, 0)
            self.assertEqual(file.created, None)


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

        self.assertEqual(cfg.action, 'run')

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

    def test_get_input_mappings_with_column_types(self):
        path = os.path.join(os.path.dirname(os.path.realpath(__file__)),
                            'data_examples', 'data4')
        cfg = Configuration(path)
        tables = cfg.tables_input_mapping
        coltypes = tables[0].column_types[0]
        source = coltypes.source
        self.assertEqual(source, "Sales")
        column_type = coltypes.type
        self.assertEqual(column_type, "VARCHAR")
        destination = coltypes.destination
        self.assertEqual(destination, "id")
        length = coltypes.length
        self.assertEqual(length, "255")
        nullable = coltypes.nullable
        self.assertEqual(nullable, False)
        convert_empty_values_to_null = coltypes.convert_empty_values_to_null
        self.assertEqual(convert_empty_values_to_null, False)

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
