import csv
from terra_bonobo_nodes import common
import unittest
from io import StringIO, BytesIO
import json
import requests
from django.contrib.gis.geos import GEOSGeometry
from bonobo.util.testing import BufferingNodeExecutionContext
from unittest import mock
from django.test import override_settings
from geostore.models import Feature, Layer


class Test_TestCommon_CsvDictReader(unittest.TestCase):
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
            row for row in csvdictreader(reader)
        ]
        self.assertSequenceEqual(tableau_rendu_csvdictreader, tableau)

    def test_csvdirectreader_vide(self):
        csvfile = BytesIO()
        csvfile.seek(0)
        reader = csvfile.read()
        csvdictreader = common.CsvDictReader()
        tableau_rendu_csvdictreader = [
            row for row in csvdictreader(reader)
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


class Test_TestCommon_GeojsonReader(unittest.TestCase):
    def setUp(self):
        self.dict_crs = {'type': 'EPSG', 'properties': {
                    'code': 4326, 'coordinate_order': [1, 0],
                    'name': 'name_to_allow'}}
        self.dict_raw_geojson_str = {'type': 'FeatureCollection', 'crs':
                                     self.dict_crs, "features": []}
        self.raw_geojson_str = json.dumps(self.dict_raw_geojson_str)

    def test_geojsonreader_error(self):
        geojsonreader = common.GeojsonReader(geom="geom")
        with self.assertRaises(ValueError):
            next(geojsonreader(self.raw_geojson_str))

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
            for row in geojsonreader(raw_geojson_str)
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
            row for row in geojsonreader(self.raw_geojson_str)]
        array_expected = []
        self.assertSequenceEqual(result_array, array_expected)


class Test_TestCommon_IdentifierFromProperty(unittest.TestCase):
    def test_identifierfromproperty(self):
        id_property = "id_prop"

        identifierproperty = common.IdentifierFromProperty(
            property=id_property)

        record_original = {
            'test': 'identifierproperty',
            id_property: 'property',
            'other': 'try'}

        identifier, record = identifierproperty(record_original)

        self.assertIn(id_property, record)
        self.assertEqual(identifier, record_original[id_property])


class Test_TestCommon_GenerateIdentifier(unittest.TestCase):
    def setUp(self):
        self.arguments = ('voici', 'les', 'arguments', 'de', 'tests')

    def test_generateidentifier_empty(self):
        generate_identifier = common.GenerateIdentifier()

        res = generate_identifier(self.arguments)

        self.assertEqual(2, len(res))
        self.assertEqual(self.arguments, res[1])
        self.assertTrue(isinstance(res[0], common.uuid.UUID))

    def test_generateidentifier_error(self):
        generate_identifier = common.GenerateIdentifier(generator=3)
        with self.assertRaises(ValueError):
            next(generate_identifier(self.arguments))

    def test_generateidentifier(self):
        generate_identifier = common.GenerateIdentifier(generator=sorted)
        array_res = generate_identifier(self.arguments)
        self.assertEqual(2, len(array_res))
        self.assertEqual(self.arguments, array_res[1])

    def test_generateidentifier_error_arguments(self):
        generate_identifier = common.GenerateIdentifier(generator=abs)
        with self.assertRaises(ValueError):
            next(generate_identifier(self.arguments))


class Test_TestCommon_ExcludeAttributes(unittest.TestCase):
    def test_excludeattributes(self):
        list_to_exclude = ['member_to_exclude_1', 'member_to_exclude_2']
        exclude_attributes = common.ExcludeAttributes(excluded=list_to_exclude)
        identifier = "id"
        record = {'member_to_exclude_1': 'exclusion',
                  'member_to_exclude_2': 'exclusion2',
                  'member_to_stay': 'stay'
                  }
        array_res = [
            row for row in exclude_attributes(identifier, record)]

        record_keys = list(array_res[0][1].keys())
        self.assertNotEqual(list_to_exclude, record_keys)
        self.assertEqual(identifier, array_res[0][0])
        self.assertEqual(2, len(array_res[0]))


class Test_TestCommon_FilterAttributes(unittest.TestCase):
    def test_filterattributes(self):
        list_to_filter = ['member_to_filter_1', 'member_to_filter_2']
        filterattributes = common.FilterAttributes(included=list_to_filter)
        identifier = "id"
        record = {'member_to_filter_1': 'filter',
                  'member_to_filter_2': 'filter2',
                  'member_to_exclude': 'exclusion'}

        result = [row for row in filterattributes(identifier, record)]

        record_keys = list(result[0][1].keys())
        self.assertEqual(list_to_filter, record_keys)
        self.assertEqual(identifier, result[0][0])
        self.assertEqual(2, len(result[0]))


class Test_TestCommon_FilterByProperties(unittest.TestCase):
    def setUp(self):
        self.identifier = "id"
        self.record = {'key_1': 'value_1',
                       'key_2': 'value_2'}

    def test_filterbyproperties_false(self):
        keep_eval_function = lambda identfier, record: False

        filterbyproperties = common.FilterByProperties(
            keep_eval_function=keep_eval_function)
        filterbyproperties(self.identifier, self.record)

        result = [
            row for
            row in filterbyproperties(self.identifier, self.record)]
        expected_result = []

        self.assertSequenceEqual(result, expected_result)

    def test_filterbyproperties_true(self):
        keep_eval_function = lambda identfier, record: True

        filterbyproperties = common.FilterByProperties(
            keep_eval_function=keep_eval_function)
        filterbyproperties(self.identifier, self.record)

        result = next(filterbyproperties(self.identifier, self.record))

        self.assertDictEqual(self.record, result[1])
        self.assertEqual(self.identifier, result[0])
        self.assertEqual(2, len(result))


class Test_TestCommon_CollectAndSum(unittest.TestCase):
    def test_collectandsum(self):
        geom = "geom"
        identifier = "identifier"

        layercollectandsum = Layer.objects.create(name="layercollectandsum")
        Feature.objects.create(geom=common.Point(2, 4),
                               layer=layercollectandsum)

        features = Feature.objects.all()

        collectandsum = common.CollectAndSum(
            geom=geom)
        id_result, features_result = next(collectandsum(identifier=identifier, features=features))
        self.assertEqual(id_result, identifier)
        self.assertIn("ids", features_result)
        self.assertIn(geom, features_result)
        self.assertIsInstance(features_result, dict)


class Test_TestCommon_MapProperties(unittest.TestCase):
    def setUp(self):
        self.identifier = "id"
        self.record = {'key_1': 'value_1',
                       'key_2': 'value_2'}

    def test_mapproperties(self):
        map_function = sorted
        mapproperties = common.MapProperties(map_function=map_function)

        result = next(mapproperties(self.identifier, self.record))
        result_expected = map_function(self.record)

        self.assertEqual(result_expected, result[1])
        self.assertEqual(self.identifier, result[0])
        self.assertEqual(2, len(result))


class Test_TestCommon_AttributeToGeometry(unittest.TestCase):
    def setUp(self):
        self.identifier = "id"
        self.geom = "geom"

        self.asso_attribute_1 = {"type": "Polygon",
                                 "coordinates": [[[3.55, 51.08], [4.36, 50.73],
                                                 [4.84, 50.85], [4.45, 51.30],
                                                 [3.55, 51.08]]]}

        self.asso_attribute_2 = {"type": "LineString",
                                 "coordinates": [[100.0, 0.0], [101.0, 1.0]]}

        self.attribute_1 = "attribute_1"
        self.attribute_2 = "attribute_2"
        self.record = {self.attribute_1: json.dumps(self.asso_attribute_1),
                       self.attribute_2: json.dumps(self.asso_attribute_2)}

    def test_get_geosgeometry(self):
        attribute_to_geometry = common.AttributeToGeometry(
            attribute=self.attribute_1, geom=self.geom)

        result = attribute_to_geometry.get_geosgeometry(
            json.dumps(self.asso_attribute_1))

        srid_expected = 4326
        self.assertEqual(result.srid, srid_expected)
        self.assertEqual(result.geom_type, self.asso_attribute_1.get("type"))

    def test_attributetogeometry(self):
        attribute_to_geometry = common.AttributeToGeometry(
            attribute=self.attribute_1, geom=self.geom)

        result = next(attribute_to_geometry(
                      self.identifier, self.record))

        result_expected_geom = self.asso_attribute_1.get("type")
        result_geom = result[1].get(self.geom).geom_type

        self.assertNotIn(self.attribute_1, self.record)
        self.assertEqual(result_expected_geom, result_geom)
        self.assertEqual(self.identifier, result[0])
        self.assertEqual(2, len(result))

    def test_attributetogeometry_linestring(self):
        attribute_to_geometry = common.AttributeToGeometry(
            attribute=self.attribute_2, geom=self.geom)
        result = next(attribute_to_geometry(
                self.identifier, self.record))

        result_expected_geom = self.asso_attribute_2.get("type")
        result_geom = result[1].get(self.geom).geom_type

        self.assertNotIn(self.attribute_2, self.record)
        self.assertEqual(result_expected_geom, result_geom)
        self.assertEqual(self.identifier, result[0])
        self.assertEqual(2, len(result))


class Test_TestCommon_AttributesToGeometry(unittest.TestCase):
    def setUp(self):
        self.attribute_1 = "1"
        self.attribute_2 = "2"
        self.identifier = "id"
        self.geom = "geom"
        self.srid = 4326
        self.y = "Key_2"
        self.x = "Key_1"

    def test_attributestogeometry(self):
        attributestopointgeometry = common.AttributesToPointGeometry(
            x=self.x, y=self.y,
            geom=self.geom, srid=self.srid)

        original_record = {'Key_1': self.attribute_1, 'Key_2': self.attribute_2}

        identifier, record = attributestopointgeometry(self.identifier, original_record)

        point_result = record.get(self.geom)

        self.assertEqual("Point", point_result.geom_type)
        self.assertEqual(self.identifier, identifier)
        self.assertEqual(float(self.attribute_1), point_result[0])
        self.assertEqual(float(self.attribute_2), point_result[1])

    def test_attributestogeometry_error(self):
        attributestopointgeometry = common.AttributesToPointGeometry(
            x=self.x, y=self.y,
            geom=self.geom, srid=self.srid)

        record = {'Key_1': "attribute_1", 'Key_2': self.attribute_2}

        with self.assertRaises(ValueError):
            next(attributestopointgeometry(self.identifier,
                                           record))


class Test_TestCommon_GeometryToJson(unittest.TestCase):
    def test_geometrytojson(self):
        source = "source"
        simplify = 0.0
        destination = "destination"
        identifier = "id"
        example_geo = common.Point(0, 1)

        geometrytojson = common.GeometryToJson(source=source,
                                               destination=destination,
                                               simplify=simplify)
        properties = {source: example_geo}

        r_identifier, record = geometrytojson(identifier, properties)

        self.assertEqual(identifier, r_identifier)
        self.assertEqual(record.get(destination).get("type"),
                         record.get(source).geom_type)


class Test_TestCommon_GeometryToCentroid(unittest.TestCase):
    def test_geometrytocentroid(self):
        example_geo = common.Polygon(((0, 0), (0, 1), (1, 1), (0, 0)))
        geom = "geom"
        geom_dest = "geom_destination"
        identifier = "id"
        properties = {geom: example_geo}

        geometrytocentroid = common.GeometryToCentroid(
            geom=geom, geom_dest=geom_dest)
        r_identifier, record = geometrytocentroid(identifier, properties)

        self.assertEqual(identifier, r_identifier)
        self.assertEqual("Point", record.get(geom_dest).geom_type)


class Test_TestCommon_Geometry3Dto2D(unittest.TestCase):
    def test_geometry3dto2d(self):
        geom = "geom"
        geom_dest = "geom_destination"
        identifier = "id"
        example_geo = common.Point(1, 1, 1)
        properties = {geom: example_geo}

        geometry3dto2d = common.Geometry3Dto2D(
            geom=geom, geom_dest=geom_dest)
        result = next(geometry3dto2d(identifier,
                                     properties))

        result_geom_3d = result[1].get(geom)
        result_geom_2d = result[1].get(geom_dest)

        self.assertEqual(identifier, result[0])
        self.assertEqual(2, len(result))
        self.assertEqual(3, len(result_geom_3d.coords))
        self.assertEqual(2, len(result_geom_2d.coords))


class Test_TestCommon_CopyOnPipelineSplit(unittest.TestCase):
    def test_copyonpipelinesplit(self):
        geom = "geom"
        example_geo = common.Point(1, 1, 1)
        properties = {geom: example_geo}
        identifier = "id"
        copyonpipelinesplit = common.CopyOnPipelineSplit()
        result = next(copyonpipelinesplit(identifier,
                                          properties))

        self.assertEqual(identifier, result[0])
        self.assertEqual(2, len(result))
        self.assertEqual(properties, result[1])


class Test_TestCommon_DropIdentifier(unittest.TestCase):
    def test_dropidentifier(self):
        geom = "geom"
        example_geo = common.Point(1, 1, 1)
        properties = {geom: example_geo}
        identifier = "id"
        dropidentifier = common.DropIdentifier()

        result = next(dropidentifier(identifier, properties))

        self.assertEqual(1, len(result))
        self.assertEqual(properties, result)


class Test_TestCommon_DjangoLog(unittest.TestCase):
    def test_djangolog(self):
        log_level = 10
        identifier = "id"

        geom_example = common.Point(4, 6)
        geom = "geom"

        record = {geom: geom_example, "Key2": "Value2"}

        djangolog = common.DjangoLog(log_level=log_level)
        with self.assertLogs(level=log_level) as cm:
            djangolog(identifier, record)
            self.assertEqual(cm.records[1].msg, f'{record["geom"].ewkt}')
            self.assertEqual(cm.records[1].levelno, log_level)
            self.assertEqual(cm.records[0].msg, f'{identifier}: {record}')
            self.assertEqual(cm.records[0].levelno, log_level)
            self.assertEqual(len(cm), 2)


class Test_TestCommon_IsochroneCalculation(unittest.TestCase):
    def setUp(self):
        self.body = {"polygons": [{"type": "Feature",
                     "properties": {"name": "Research Triangle", "area": 252},
                                   "geometry": {"type": "Polygon", "coordinates":
                                                [[[-78.93, 36.00], [-78.67, 35.78],
                                                 [-79.04, 35.90], [-78.93, 36.00]]]}}]}

    @override_settings(GRAPHHOPPER="http://graphopper/", DEBUG=True)
    def test_isochronecalculation_valid(self):
        request = requests.Session()
        with mock.patch.object(request, 'get',
                               return_value=mock.Mock(ok=True))as mock_get:
            mock_get.return_value.json.return_value = self.body

            isochronecalculation = common.IsochroneCalculation()
            identifier = "id"
            geom_example = common.Point(4, 6)

            properties = {"geom": geom_example}

            result = next(isochronecalculation(identifier,
                                               properties,
                                               request))
            self.assertEqual(identifier, result[0])
            self.assertEqual(2, len(result))

    @override_settings(GRAPHHOPPER="http://graphopper/", DEBUG=True)
    def test_isochronecalculation_non_valid(self):
        request = requests.Session()

        with mock.patch.object(request, 'get',
                               return_value=mock.Mock(ok=True))as mock_get:

            mock_get.return_value.json = mock.MagicMock(
                side_effect=json.JSONDecodeError("test", "test2", 123))

            isochronecalculation = common.IsochroneCalculation()
            identifier = "id"
            geom_example = common.Point(4, 6)

            properties = {"geom": geom_example}

            try:
                with self.assertLogs():
                    next(isochronecalculation(identifier,
                                              properties,
                                              request))
            except StopIteration:
                pass


class Test_TestCommon_UnionOnProperty(unittest.TestCase):
    def test_uniononproperty(self):
        atr = 'atr'
        properties = [{
            'geom': common.Point(1, 0),
            atr: 32
        }, {
            'geom': common.Point(2, 0),
            atr: 33
        }, {
            'geom': common.Point(3, 0),
            atr: 32
        }, {
            'geom': common.LineString([(0, 0), (1, 0)]),
            atr: 32
        }, {
            'geom': common.Point(4, 0),
            atr: 32
        }
        ]

        with BufferingNodeExecutionContext(common.UnionOnProperty(
                                        property=atr)) as context:
            for row in properties:
                context.write_sync(("id", row))

        result = dict(context.get_buffer())

        for row in properties:
            select_result = result.get(row.get(atr))
            select_result_geom = select_result.get("geom")

            self.assertEqual(row.get(atr),
                             select_result.get("level"))
            self.assertTrue(select_result_geom.intersects(row.get("geom")))


if __name__ == '__main__':
    unittest.main()
