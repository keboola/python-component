import unittest

from keboola.component.sync_actions import SelectElement, ValidationResult, process_sync_action_result, MessageType


class TestSyncActions(unittest.TestCase):

    def test_select_element_return_value(self):
        select_options = [SelectElement("value_a", "label_a"),
                          SelectElement("value_b")]
        expected = '[{"value": "value_a", "label": "label_a"}, {"value": "value_b", "label": "value_b"}]'
        self.assertEqual(process_sync_action_result(select_options), expected)

    def test_select_element_return_value_legacy(self):
        select_options = [dict(value="value_a", label="label_a"),
                          dict(value="value_b", label="value_b")]
        expected = '[{"value": "value_a", "label": "label_a"}, {"value": "value_b", "label": "value_b"}]'
        self.assertEqual(process_sync_action_result(select_options), expected)

    def test_validation_result_value(self):
        result = ValidationResult("Some Message", MessageType.WARNING)
        expected = '{"message": "Some Message", "type": "warning", "status": "success"}'
        self.assertEqual(process_sync_action_result(result), expected)

        # default type
        result = ValidationResult("Some Message")
        expected = '{"message": "Some Message", "type": "info", "status": "success"}'
        self.assertEqual(process_sync_action_result(result), expected)
