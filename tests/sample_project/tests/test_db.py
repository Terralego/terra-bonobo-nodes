from terra_bonobo_nodes import db
import unittest
from geostore.models import Layer


class Test_TestDB_KeyFloatTransform(unittest.TestCase):
    def test_keyfloattransform(self):
        layer = Layer.objects.create(name="layerkeyfloattransform")
        queryset = Layer.objects.filter(pk=layer.pk)

        compiler = queryset.query.get_compiler(queryset.db)
        properties = 5
        query = "SELECT"
        keyfloattransform = db.KeyFloatTransform(query, properties)
        string_result, param_result = keyfloattransform.as_sql(
            compiler, "properties")
        self.assertEqual(param_result, (properties, 'SELECT'))
        self.assertIsInstance(string_result, str)
        self.assertIn("::float", string_result)


if __name__ == '__main__':
    unittest.main()
