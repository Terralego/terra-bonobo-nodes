from terra_bonobo_nodes import sql
import unittest
import json


class Test_TestSql_SQLExtract(unittest.TestCase):
    def test_sql_extractvalid(self):
        db_alias = "default"
        identifier = "a"
        sql_query = '''
            SELECT 
            123::numeric AS A,
            456::numeric AS B
        '''
        sqlextract = sql.SQLExtract(db_alias=db_alias,
                                    identifier=identifier,
                                    sql_query=sql_query)
        result = next(sqlextract())
        self.assertEqual(len(result), 2)
        self.assertIsInstance(result[1], dict)
        self.assertNotIn(identifier, result[1])
        self.assertNotIn(result[0], result[1])


    def test_attributefromsql_valid(self):
        db_alias = "default"
        identifier = "identifier"
        property_ = "property"
        record = {
            property_: "example_value"
        }

        sql_query = "SELECT 1::numeric"

        sqlextract = sql.AttributeFromSQL(db_alias=db_alias,
                                          property=property_,
                                          sql_query=sql_query)
        result = sqlextract(identifier, record)

        self.assertEqual(len(result), 2)
        self.assertEqual(result[0], identifier)
        self.assertIsInstance(result[1], dict)
        self.assertIn(property_, result[1])
        self.assertEqual(len(result[1]), 1)
