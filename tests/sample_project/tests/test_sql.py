from terra_bonobo_nodes import sql
import unittest


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
        id_result, properties_result = next(sqlextract())
        self.assertIsInstance(properties_result, dict)
        self.assertIn(identifier, properties_result)
        self.assertNotIn(id_result, properties_result)

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
        id_result, record_result = sqlextract(identifier, record)

        self.assertEqual(id_result, identifier)
        self.assertIsInstance(record_result, dict)
        self.assertIn(property_, record_result)
        self.assertEqual(len(record_result), 1)
