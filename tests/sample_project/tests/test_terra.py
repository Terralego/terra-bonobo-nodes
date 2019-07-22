from terra_bonobo_nodes import terra
import unittest
from django.contrib.postgres.fields import JSONField
from django.db import connections
from django.contrib.gis.geos import Point, Polygon, GEOSGeometry
from bonobo.util.testing import BufferingNodeExecutionContext
from bonobo.config import Service
from unittest import mock
from django.test import override_settings
import requests
from json import JSONDecodeError
from django.utils import timezone


class Test_TestTerra_LayerClusters(unittest.TestCase):
    def test_layer_cluster(self):
        metric_projection_srid = 4326
        distance = 2

        layer_1_name = "layer1"
        layer_1 = terra.Layer(id=1, name=layer_1_name)
        layer_1.save()
        layer_2_name = "layer2"
        layer_2 = terra.Layer(id=2, name=layer_2_name)
        layer_2.save()
        layer_3_name = "layer3"
        layer_3 = terra.Layer(id=1, name=layer_3_name)

        geom_1 = Point(4, 6)
        geom_2 = Point(6, 4)
        geom_3 = Point(2, 4)

        terra.Feature.objects.create(
            geom=geom_1, identifier="identifier1",
            layer=layer_1, source=1, target=1)
        terra.Feature.objects.create(
            geom=geom_2, identifier="identifier2",
            layer=layer_2, source=2, target=2)
        terra.Feature.objects.create(
            geom=geom_3, identifier="identifier3",
            layer=layer_3, source=3, target=3)

        input_layers = [layer_1, layer_2]
        layer_cluster = terra.LayerClusters(
            input_layers=input_layers,
            metric_projection_srid=metric_projection_srid,
            distance=distance)
        result = [row for row in layer_cluster()]
        for row in result: 
            self.assertEqual(len(row), 2)
            self.assertIsInstance(row[0], str)
            self.assertIsInstance(row[1], terra.FeatureQuerySet)

    def test_subdividegeom(self):
        geom_value = Point(4, 6)
        subdividegeom = terra.SubdivideGeom()
        properties = {'geom': geom_value, 'example_key': 'example_value'}
        identifier = "identifier"
        subdividegeom(properties=properties, identifier=identifier)
        result = [row for row in subdividegeom(properties=properties,
                  identifier=identifier)]
        for row in result:
            self.assertEqual(len(row), 2)
            self.assertIsInstance(row[0], str)
            self.assertIsInstance(row[1], dict)

    def test_loadfeaturegeom(self):
        id_ = "identifier"
        record = {"a": "b", "c": "d"}
        layer_name = "layer10"
        layer = terra.Layer.objects.create(id=10, name=layer_name)
        with BufferingNodeExecutionContext(
                terra.LoadFeatureInLayer(),
                services={'output_layer': layer}) as context:
            context.write_sync((id_, record))

        result = context.get_buffer()
        # print(result)

    def test_getfatureobject(self):
        record = {
            "geom": "value_geom",
            "key_2": "value_2",
        }
        identifier = "identifier"
        loadfeaturelayer = terra.LoadFeatureInLayer()
        layer = terra.Layer.objects.create(id=100, name='layer_geat_feature')
        loadfeaturelayer.write_layer = layer
        result = loadfeaturelayer._get_feature_object(identifier, record)
        self.assertIsInstance(result, terra.Feature)

    def test_extractfeatures(self):
        layer111 = terra.Layer.objects.create(id=111, name="layer_111")
        layer121 = terra.Layer.objects.create(id=121, name="layer_121")

        geom_1 = Point(4, 6)
        geom_2 = Point(6, 4)

        terra.Feature.objects.create(
            geom=geom_1, identifier="identifier111",
            layer=layer111, source=1, target=1)
        terra.Feature.objects.create(
            geom=geom_2, identifier="identifier121",
            layer=layer121, source=2, target=2)

        queryset = terra.Feature.objects.all()
        extractfeature = terra.ExtractFeatures(queryset=queryset)
        extractfeature.batch_size = 1
        result = [row for row in extractfeature()]
        for row in result:
            self.assertEqual(len(row), 2)
            self.assertIsInstance(row[0], str)
            self.assertIsInstance(row[1], dict)

    def test_booleanintersect_exception(self):
        layer_nom = "layer"
        property_ = "property"
        terra.Layer.objects.create(id=11, name=layer_nom)
        booleanintersect = terra.BooleanIntersect(
                layer=layer_nom, property=property_)
        record = {"key_example": "value_example"}
        with self.assertLogs():
            next(booleanintersect(identifier='identifier', record=record))
            self.assertEqual(record[property_], False)

    def test_booleanintersect_valid(self):
        layer_nom = "layer_valid"
        property_ = "property123"
        layer = terra.Layer.objects.create(id=12, name=layer_nom)
        geom_1 = Point(4, 6)
        identifier = "id"
        terra.Feature.objects.create(
            geom=geom_1, identifier="identifier1", layer=layer,
            source=1, target=1)

        record = {
            'geom': geom_1,
        }

        booleanintersect = terra.BooleanIntersect(
                layer=layer_nom, property=property_)
        result = next(booleanintersect(identifier=identifier, record=record))

        self.assertEqual(len(result), 2)
        self.assertEqual(identifier, result[0])
        self.assertTrue(result[1][property_])

    def test_intersectionpercentbyarea_exception(self):
        layer = "layer"
        property_ = "property"
        record = {"property": "value_example"}
        intersectionpercentbyarea = terra.IntersectionPercentByArea(
            layer=layer, property=property_)
        with self.assertLogs():
            next(intersectionpercentbyarea(
                identifier='identifier', record=record))

    def test_intersectionpercentbyarea_valid(self):
        layer_nom = "layer_autre_nom"
        property_ = "property"
        geom = "geom"
        identifier = "id"
        geom_1 = Polygon(((0.0, 0.0), (0.0, 1.0), (1.0, 1.0), (1.0, 0.0),
                          (0.0, 0.0)))
        record = {
            geom: geom_1,
        }
        layer = terra.Layer.objects.create(id=13, name=layer_nom)
        terra.Feature.objects.create(
            geom=geom_1, identifier="identifier1", layer=layer,
            source=1, target=1)

        intersectionpercentbyarea = terra.IntersectionPercentByArea(
            layer=layer_nom, property=property_, geom=geom)

        result = next(intersectionpercentbyarea(
            identifier=identifier, record=record))

        self.assertEqual(len(result), 2)
        self.assertEqual(identifier, result[0])
        self.assertIsInstance(result[1][property_], float)

    def test_closestfeatures_attribute_valid(self):
        layer_nom = "layer_other_name"
        terra.Layer.objects.create(id=14, name=layer_nom)
        identifier = "id"
        geom_1 = Polygon(((0.0, 0.0), (0.0, 1.0), (1.0, 1.0), (1.0, 0.0),
                          (0.0, 0.0)), srid=4366)
        properties = {
            'geom': geom_1,
        }
        closestfeatures = terra.ClosestFeatures(layer=layer_nom, max_distance=5)
        result = closestfeatures(identifier=identifier,
                                 properties=properties)

        self.assertEqual(len(result), 2)
        self.assertEqual(identifier, result[0])
        self.assertIsInstance(result[1]['closests'], list)

    def test_closestfeatures_attribute_error(self):
        layer_nom = "layer_15"
        layer = terra.Layer.objects.create(id=15, name=layer_nom)
        identifier = "id"
        geom_1 = Polygon(((0.0, 0.0), (0.0, -1.0), (-1.0, 1.0), (1.0, 0.0),
                          (0.0, 0.0)), srid=4366)
        properties = {
            'geom': geom_1,
        }
        property_filter = {1: 'error'}

        terra.Feature.objects.create(
            geom=geom_1, identifier="identifier1", layer=layer,
            source=1, target=1)
        closestsfeatures = terra.ClosestFeatures(
            layer=layer, property_filter=property_filter)
        with mock.patch.object(terra.Layer.objects, 'get',
                               side_effect=AttributeError('foo')):
            result = closestsfeatures(
                identifier=identifier, properties=properties)
            self.assertEqual(len(result), 2)
            self.assertEqual(identifier, result[0])
            self.assertNotIn('closests', result[1])

    def test_transittimeonetomany_exception(self):
        transittimeonetomany = terra.TransitTimeOneToMany()
        identifier = "id"
        properties = {
            'points': [Point(5, 6), Point(7, 6)],
            'geom': Polygon(((0.0, 0.0), (0.0, 1.0), (1.0, 1.0), (1.0, 0.0),
                             (0.0, 0.0)), srid=4366)
        }

        request = requests.Session()
        with mock.patch.object(request, 'get',
                               return_value=mock.Mock(ok=True))as mock_get:
            mock_get.return_value.json = mock.MagicMock(
                side_effect=JSONDecodeError("test", "test2", 123))
            result = transittimeonetomany(identifier, properties, request)
            self.assertEqual(2, len(result))
            self.assertEqual(result[0], identifier)
            for row in result[1]['times']:
                self.assertEqual(row, [None])

    def test_transittimeonetomany_valid(self):
        transittimeonetomany = terra.TransitTimeOneToMany()
        identifier = "id"
        properties = {
            'points': [Point(5, 6), Point(7, 6),
                       Point(8, 6), Point(9, 6), Point(10, 6)],
            'geom': Polygon(((0.0, 0.0), (0.0, 1.0), (1.0, 1.0), (1.0, 0.0),
                             (0.0, 0.0)), srid=4366)
        }
        request = requests.Session()
        with mock.patch.object(request, 'get',
                               return_value=mock.Mock(ok=True))as mock_get:
            mock_get.return_value.json.return_value = {
                'paths': [{
                    'fastest': 5
                }]}
            result = transittimeonetomany(identifier, properties, request)
            self.assertEqual(2, len(result))
            self.assertEqual(result[0], identifier)
            for row in result[1]['times']: 
                self.assertIsInstance(row, list)

    def test_transittimeonetoone_else(self):
        identifier = "id"
        properties = {
            'points': [],
            'geom': Polygon(((0.0, 0.0), (0.0, 1.0), (1.0, 1.0), (1.0, 0.0),
                             (0.0, 0.0)), srid=4366)
        }
        request = requests.Session()
        transittimeonetoone = terra.TransitTimeOneToOne()
        with mock.patch.object(request, 'get',
                               return_value=mock.Mock(ok=True))as mock_get:
            mock_get.return_value.json.return_value = {
                'paths': [{
                    'fastest': 5
                }]}
            result = transittimeonetoone(
                identifier=identifier, properties=properties, http=request)
            self.assertEqual(len(result), 2)
            self.assertEqual(result[0], identifier)
            self.assertEqual(result[1]['times'], None)

    def test_transittimeonetoone_if(self):
        identifier = "id"
        properties = {
            'points': [Point(5, 6), Point(7, 6),
                       Point(8, 6), Point(9, 6), Point(10, 6)],
            'geom': Polygon(((0.0, 0.0), (0.0, 1.0), (1.0, 1.0), (1.0, 0.0),
                             (0.0, 0.0)), srid=4366)
        }
        request = requests.Session()
        transittimeonetoone = terra.TransitTimeOneToOne()
        with mock.patch.object(request, 'get',
                               return_value=mock.Mock(ok=True))as mock_get:
            mock_get.return_value.json.return_value = {
                'paths': [{
                    'fastest': 5
                }]}
            result = transittimeonetoone(
                identifier=identifier, properties=properties, http=request)
            self.assertEqual(len(result), 2)
            self.assertEqual(result[0], identifier)
            self.assertIsInstance(result[1], dict)

    def test_accessibilityratiobytime_if(self):
        time_limits = [1, 2, 3]
        property_ = "property"
        identifier = "identifier"
        properties = {
            'times': False
        }

        accessibilityratiobytime = terra.AccessibilityRatioByTime(
            time_limits=time_limits, property=property_)
        result = accessibilityratiobytime(
            identifier=identifier, properties=properties)
        self.assertEqual(len(result), 2)
        self.assertEqual(result[0], identifier)
        self.assertIsInstance(result[1], dict)
        self.assertNotIn('times', result[1])

    def test_accessibilityratiobytime(self):
        time_limits = [11, 22, 33, 44, 55]
        property_ = "property"
        identifier = "identifier"
        properties = {
            'times': [[111, 222, 333, 444, 555], [111, 222, 333, 444, 555]
            , [111, 222, 333, 444, 555], [111, 222, 333, 444, 555], [111, 222, 333, 444, 555]],
            'example_key': 'example_value',
            'example_key_2': 'example_value_2',
            'example_key_3': 'example_value_3',
            'example_key_4': 'example_value_4',
            'example_key_5': 'example_value_5',
        }

        # time = properties['times']
        # for n in range(0, 5):
        #     for mode_i, limit in enumerate(time_limits):
        #         print("mode_i:")
        #         print(mode_i)
        #         print("limit:")
        #         print(limit)

        accessibilityratiobytime = terra.AccessibilityRatioByTime(
            time_limits=time_limits, property=property_)
        result = accessibilityratiobytime(
            identifier=identifier, properties=properties)

        # print(result)
        self.assertEqual(len(result), 2)
        self.assertEqual(result[0], identifier)
        # print("Result1 : ")
        # print(result[1][property_])
        self.assertIsInstance(result[1][property_], float)

    def test_simplifygeom(self):
        tolerance = 5
        geom_in = "geom_in"
        geom_out = "geom_out"
        identifier = "identifier"
        record = {
            geom_in: Point(0, 1)
        }
        geom_type = record[geom_in].geom_type
        simplifygeom = terra.SimplifyGeom(
            tolerance=tolerance, geom_in=geom_in, geom_out=geom_out)
        result = simplifygeom(identifier=identifier, record=record)

        self.assertEqual(len(result), 2)
        self.assertEqual(result[0], identifier)
        self.assertEqual(result[1][geom_out].geom_type, geom_type)

    def test_transformgeom(self):
        ct = '''GEOGCS["GCS_HD1909",DATUM["D_Hungarian_Datum_1909",
        SPHEROID["Bessel_1841",6377397.155,299.1528128]],
        PRIMEM["Greenwich",0.0],UNIT["Degree",0.0174532925199433]]'''
        geom_in = "geom_in"
        geom_out = "geom_out"
        identifier = "identifier"
        record = {
            geom_in: Point(0, 1, srid=4326)
        }
        geom_type = record[geom_in].geom_type
        transformgeom = terra.TransformGeom(
            ct=ct, geom_in=geom_in, geom_out=geom_out)
        result = transformgeom(identifier=identifier, record=record)

        self.assertEqual(len(result), 2)
        self.assertEqual(result[0], identifier)
        self.assertEqual(result[1][geom_out].geom_type, geom_type)

    def test_cleanolderthan(self):
        time = timezone.now()
        identifier = "identifier"
        properties = {
            "exemple_key": "exemple_value"
        }
        terra.Layer.objects.create(id=16, name='layer_16')
        terra.Layer.objects.create(id=17, name='layer_17')
        layer = terra.Layer.objects.create(id=18, name='layer_18')
        with BufferingNodeExecutionContext(terra.CleanOlderThan(
                                time=time), services={'output_layer': layer})as context:
            context.write_sync((identifier, properties))
        result = context.get_buffer()
        for row in result:
            self.assertEqual(len(row), 2)
            self.assertEqual(row[0], identifier)
            self.assertEqual(row[1], properties)


if __name__ == '__main__':
    unittest.main()
