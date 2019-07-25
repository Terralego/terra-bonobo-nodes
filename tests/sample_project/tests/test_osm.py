from terra_bonobo_nodes import osm
import unittest
import json
import requests
from unittest import mock


class Test_TestOsm_OverpassExtract(unittest.TestCase):
    def test_osm_overpassextract_logg_error(self):
        query = "query"
        request = requests.Session()
        overpassextract = osm.OverpassExtract(query=query)
        with mock.patch.object(request, 'post',
                               return_value=mock.Mock(ok=False)):
            with self.assertLogs(), self.assertRaises(RuntimeError):
                next(overpassextract(request))

    def test_osm_overpassextract_valid(self):
        query = "query"
        request = requests.Session()
        with mock.patch.object(request, 'post',
                               return_value=mock.Mock(ok=True)) as mock_post:

            overpassextract = osm.OverpassExtract(query=query)
            content = "test_decode".encode('utf-8')

            mock_post.return_value = mock.Mock(
                content=content)
            result = next(overpassextract(request))
            self.assertEqual(result, content.decode('utf-8'))


class Test_TestOsm_OsmXMLtoGeojson(unittest.TestCase):
    def test_osmxmltogeojson_not_valid_returncode(self):
        with mock.patch.object(osm.subprocess, 'run',
                               return_value=mock.Mock(ok=True)) as mock_returncode:
            mock_returncode.return_value.poll.return_value = 1
            string = '''<?xml version="1.0" encoding="UTF-8"?>
            <osm version="0.6" generator="CGImap 0.0.2">
            </osm> '''
            content = bytes(string, 'utf-8')
            osmxmltogeojson = osm.OsmXMLtoGeojson("points")
            with self.assertRaises(RuntimeError), self.assertLogs():
                next(osmxmltogeojson(content))

    def test_osmxmltogeojson_not_valid_calledprocessederror(self):
        with mock.patch.object(
            osm.subprocess,
            'run',
            side_effect=osm.subprocess.CalledProcessError(returncode=1,
                                                          cmd=["wrong"])):
            content = bytes('''<?xml version="1.0" encoding="UTF-8"?>
            <osm version="0.6" generator="CGImap 0.0.2">
            </osm>''', 'utf-8')
            osmxmltogeojson = osm.OsmXMLtoGeojson("points")
            with self.assertRaises(RuntimeError):
                next(osmxmltogeojson(content))

    def test_osmxmltogeojson_valid(self):
        type_feature = "points"
        osmxmltogeojson = osm.OsmXMLtoGeojson(type_features="points")
        string = '''<?xml version="1.0" encoding="UTF-8"?>

        <osm version="0.6" generator="CGImap 0.0.2">

        <node id="1831881213" version="1" changeset="12370172" lat="54.0900666"
        lon="12.2539381" user="lafkor" uid="75625" visible="true"
        timestamp="2012-07-20T09:43:19Z">

        <tag k="name" v="Neu Broderstorf"/>

        <tag k="traffic_sign" v="city_limit"/>

        </node>

        </osm> '''

        content = bytes(string, 'utf-8')
        result = json.loads(next(osmxmltogeojson(content)))
        self.assertEqual(type_feature, result.get("name"))
        for row in result.get("features"):
            self.assertEqual(row.get("type"), "Feature")


class Test_TestOsm_Ogr2ogrGeojson2Geojson(unittest.TestCase):
    def test_ogr2ogrgeojson2geojson_valid(self):
        ogr2ogrgeojson2geojson = osm.Ogr2ogrGeojson2Geojson()
        other_tags = '''"frequency"=>"50","gauge"=>"6666","layer"=>"19"'''
        record = {"other_tags": other_tags}
        result = next(ogr2ogrgeojson2geojson(record))

        self.assertEqual(len(result), other_tags.count("=>"))
        self.assertEqual(type(record), type(result))

    def test_ogr2ogrgeojson2geojson_non_valid_valuerror(self):
        ogr2ogrgeojson2geojson = osm.Ogr2ogrGeojson2Geojson()
        other_tags = "test"
        record = {"other_tags": other_tags}
        with self.assertRaises(ValueError):
            next(ogr2ogrgeojson2geojson(record))

    def test_ogr2ogrgeojson2geojson_non_valid_keyerror(self):
        ogr2ogrgeojson2geojson = osm.Ogr2ogrGeojson2Geojson()
        other_tags = '''"frequency"=>"50","gauge"=>"6666","layer"=>"19"'''
        record = {'non_valid_key': other_tags}
        result = next(ogr2ogrgeojson2geojson(record))
        self.assertEqual(result, record)


if __name__ == '__main__':
    unittest.main()
