import unittest

from keboola.component.dao import *


class TestTableMetadata(unittest.TestCase):
    def test_column_metadata_is_valid(self):
        tmetadata = TableMetadata()
        pass

    # TODO: Test dao.TableMetadata.column_metadata produces valid column metadata structure
    """
    "column_metadata": {
        "order_id": [],
        "order_date": [
            {
                "id": "596588515",
                "key": "KBC.datatype.basetype",
                "value": "DATE",
                "provider": "user",
                "timestamp": "2020-11-05T09:05:17+0100"
            },
            {
                "id": "596588923",
                "key": "KBC.description",
                "value": "datum",
                "provider": "user",
                "timestamp": "2020-11-05T09:05:24+0100"
            }
        ]
    """

    def test_table_metadata_is_valid(self):
        pass
        # TODO: Test dao.TableMetadata.table_metadata produces valid column metadata structure
        # "metadata": [
        #         {
        #             "id": "228956",
        #             "key": "KBC.createdBy.component.id",
        #             "value": "keboola.python-transformation",
        #             "provider": "system",
        #             "timestamp": "2017-05-26 00:39:07"
        #         }
        #     ]

    # TODO: test other TableMetadata methods (uniqueness of desc,basetype; proper keys, etc)
