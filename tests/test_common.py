import csv
from terra_bonobo_nodes import common
import unittest
from io import StringIO, BytesIO
import json
from django.contrib.gis.geos import GEOSGeometry


class Test_TestCommon(unittest.TestCase):
    def setUp(self):
        self.dict_crs = {'type': 'EPSG', 'properties': {
                    'code': 4326, 'coordinate_order': [1, 0],
                    'name': 'name_to_allow'}}
        self.dict_raw_geojson_str = {'type': 'FeatureCollection', 'crs':
                                     self.dict_crs, "features": []}
        self.raw_geojson_str = json.dumps(self.dict_raw_geojson_str)

    def test_csvdirectreader(self):
        csvfile = StringIO()
        fieldnames = ['Test1']
        writer = csv.DictWriter(csvfile, fieldnames=fieldnames)
        writer.writeheader()
        tableau = [
            {'Test1': 'test1'},
            {'Test1': "iklojlk"},
        ]
        for i in tableau:
            writer.writerow(i)
        csvfile.seek(0)
        reader = csvfile.read()
        csvdictreader = common.CsvDictReader()
        tableau_rendu_csvdictreader = [
            row for row in csvdictreader.__call__(reader)
        ]
        self.assertSequenceEqual(tableau_rendu_csvdictreader, tableau)

    def test_csvdirectreader_vide(self):
        csvfile = BytesIO()
        csvfile.seek(0)
        reader = csvfile.read()
        csvdictreader = common.CsvDictReader()
        tableau_rendu_csvdictreader = [
            row for row in csvdictreader.__call__(reader)
        ]
        self.assertSequenceEqual(tableau_rendu_csvdictreader, [])

    def test_dialect(self):
        dialecte_expected = {'delimiter': ':', 'quotechar': '"', 'escapechar':
                             'True', 'doublequote': 'False',
                             'skipinitialspace': 'True',
                             'lineterminator': '\n', 'quoting': 0}
        csvdictreader_dialect = common.CsvDictReader(**dialecte_expected)
        self.assertDictEqual(dialecte_expected,
                             csvdictreader_dialect.get_dialect_kwargs())

    def test_geojsonreader_error(self):
        geojsonreader = common.GeojsonReader(geom="geom")
        with self.assertRaises(ValueError):
            geojsonreader.__call__(self.raw_geojson_str).__next__()

    def test_geojsonreader(self):
        dic_geometry_1 = {'type': 'LineString', 'coordinates':
                          [[102.0, 0.0], [103.0, 1.0],
                           [104.0, 0.0], [105.0, 1.0]]}
        dict_feature_1 = {'type': 'feature', "id": "id0",
                          'geometry': dic_geometry_1, 'properties':
                          {'prop0': 'value0', 'prop1': 'value1'}}
        self.dict_crs.get("properties").update({"name":
                                                'urn:ogc:def:crs:OGC:1.3:CRS84'
                                                })
        self.dict_crs.update({"properties": self.dict_crs.get("properties")})
        self.dict_raw_geojson_str.update({"features": [dict_feature_1]})
        self.dict_raw_geojson_str.update({"crs": self.dict_crs})
        raw_geojson_str = json.dumps(self.dict_raw_geojson_str)
        geojsonreader = common.GeojsonReader(geom="geom")

        result_geo_array = [
            row.get('geom')
            for row in geojsonreader.__call__(raw_geojson_str)
        ]

        expected_array = [
            GEOSGeometry(json.dumps(row.get('geometry')))
            for row in self.dict_raw_geojson_str.get('features')
        ]
        self.assertSequenceEqual(expected_array, result_geo_array)

    def test_geojsonreader_empty(self):
        name_allowed = self.dict_crs.get('properties').get('name')
        geojsonreader = common.GeojsonReader(geom="geom",
                                             allowed_projection=[name_allowed])
        result_array = [
            row for row in geojsonreader.__call__(self.raw_geojson_str)]
        array_expected = []
        self.assertSequenceEqual(result_array, array_expected)

    def test_identifierfromproperty(self):
        property_to_remove = "property_to_remove"

        identifierproperty = common.IdentifierFromProperty(
            property=property_to_remove)

        record_original = {
            'test': 'identifierproperty',
            'property_to_remove': 'property',
            'other': 'try'}
        value_expected = record_original.get(property_to_remove)

        dict_res = [
            row for row in identifierproperty.__call__(record_original)]

        self.assertNotIn(property_to_remove, dict_res[0][1])
        self.assertEqual(value_expected, dict_res[0][0])
        self.assertEqual(2, len(dict_res[0]))



if __name__ == '__main__':
    unittest.main()
