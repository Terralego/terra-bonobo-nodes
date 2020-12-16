from terra_bonobo_nodes import shapefile
import unittest
import json
import os
from unittest import mock


class Test_TestShapefile_ZipShapefileToGeojson(unittest.TestCase):
    def test_zipshapefiletogeojson_not_valid_returncode(self):
        with mock.patch.object(shapefile.subprocess, 'run',
                               return_value=mock.Mock(ok=True)) as mock_returncode:
            mock_returncode.return_value.poll.return_value = 1
            string = " "
            content = bytes(string, 'utf-8')
            zipshapefiletogeojson = shapefile.ZipShapefileToGeojson()
            with self.assertRaises(RuntimeError), self.assertLogs():
                next(zipshapefiletogeojson(content))

    def test_zipshapefiletogeojson_not_valid_calledprocessederror(self):
        with mock.patch.object(
            shapefile.subprocess,
            'run',
            side_effect=shapefile.subprocess.CalledProcessError(returncode=1,
                                                          cmd=["wrong"])):
            content = bytes("", 'utf-8')
            zipshapefiletogeojson = shapefile.ZipShapefileToGeojson()
            with self.assertRaises(RuntimeError):
                next(zipshapefiletogeojson(content))

    def test_zipshapefiletogeojson_valid(self):
        zipshapefiletogeojson = shapefile.ZipShapefileToGeojson()
        HERE = os.path.dirname(__file__)
        zfile = os.path.join(HERE, "Bank_Reconfiguration_and_Basking_Features_Area.zip")
        with open(zfile, 'rb') as my_zip:
            zip_read = my_zip.read()
            result = json.loads(next(zipshapefiletogeojson(zip_read)))
        for row in result.get("features"):
            self.assertEqual(row.get("type"), "Feature")



if __name__ == '__main__':
    unittest.main()
