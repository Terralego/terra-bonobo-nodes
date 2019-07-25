from terra_bonobo_nodes import db
import unittest
from terracommon.terra.models import Layer


class Test_TestDB_KeyFloatTransform(unittest.TestCase):
    def test_keyfloattransform(self):
        Layer.objects.create(name="layerkeyfloattransform")
        queryset = Layer.objects.all()
        compiler = queryset.query.get_compiler(queryset.db)
        properties = 5
        query = "SELECT"
        keyfloattransform = db.KeyFloatTransform(query, properties)
        string_result, param_result = keyfloattransform.as_sql(
            compiler, "properties")
        self.assertEqual(param_result, [properties])
        self.assertIsInstance(string_result, str)
        self.assertIn(query, string_result)
        self.assertIn("::float", string_result)


if __name__ == '__main__':
    unittest.main()
