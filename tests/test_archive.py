from terra_bonobo_nodes import archive
import os
import unittest


HERE = os.path.dirname(__file__)


class Test_TestOsm_OverpassExtract(unittest.TestCase):
    def test_zipreader_empty_path(self):
        zfile = os.path.join(HERE, 'School_Property.zip')
        zipreader = archive.ZipReader()
        with open(zfile, 'rb') as my_zip:
            zip_read = my_zip.read()
            result = [row for row in zipreader(zip_read)]
        size_expected = len(archive.ZipFile(zfile).namelist())
        self.assertEqual(size_expected, len(result))
        for row in result:
            self.assertIsInstance(row[0], str)
            self.assertIsInstance(row[1], bytes)

    def test_zipreader_field_paths(self):
        zfile = os.path.join(HERE, 'School_Property.zip')
        content_path = ["School_Property.shp"]
        zipreader = archive.ZipReader(content_path)
        with open(zfile, 'rb') as my_zip:
            zip_read = my_zip.read()
            result = [row for row in zipreader(zip_read)]
        self.assertEqual(len(content_path), len(result))
        for row in result:
            self.assertIsInstance(row[0], str)
            self.assertIsInstance(row[1], bytes)


if __name__ == '__main__':
    unittest.main()
