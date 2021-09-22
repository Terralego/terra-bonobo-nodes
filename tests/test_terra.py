from terra_bonobo_nodes import terra
from django.contrib.gis.geos import Point, Polygon
from bonobo.util.testing import BufferingNodeExecutionContext
from unittest import mock
import requests
from json import JSONDecodeError
from django.utils import timezone
import django


class Test_TestTerra_LayerClusters(django.test.TestCase):
    def setUp(self):
        self.geometries = {
            "layer1": Point(4, 6),
            "layer2": Point(6, 4),
            "layer3": Point(2, 4),
            "layerpolygon": Polygon(
                ((0.0, 0.0), (0.0, 1.0), (1.0, 1.0), (1.0, 0.0), (0.0, 0.0)), srid=4326
            ),
        }
        self.layers = []
        for layer_name, geom in self.geometries.items():
            self.layers += [terra.Layer.objects.create(name=layer_name)]
            terra.Feature.objects.create(geom=geom, layer=self.layers[-1])

    def test_layer_cluster(self):
        metric_projection_srid = 4326
        distance = 2

        input_layers = [self.layers[0], self.layers[1]]
        layer_cluster = terra.LayerClusters(
            input_layers=input_layers,
            metric_projection_srid=metric_projection_srid,
            distance=distance,
        )
        result = [row for row in layer_cluster()]
        for row in result:
            cluster_result, features_result = row
            self.assertIsInstance(cluster_result, str)
            self.assertIsInstance(features_result, terra.FeatureQuerySet)

    def test_subdividegeom(self):
        subdividegeom = terra.SubdivideGeom()
        properties = {
            "geom": next(iter(self.geometries.values())),
            "example_key": "example_value",
        }
        identifier = "identifier"
        subdividegeom(properties=properties, identifier=identifier)
        result = [
            row for row in subdividegeom(properties=properties, identifier=identifier)
        ]
        for row in result:
            id_result, properties_result = row
            self.assertIsInstance(id_result, str)
            self.assertEqual(properties_result, properties)

    def test_loadfeatureinlayer(self):
        id_ = "identifier"
        record = {"a": "b", "c": "d"}
        with BufferingNodeExecutionContext(
            terra.LoadFeatureInLayer(layer_name=self.layers[0].name)
        ) as context:
            context.write_sync((id_, record))

        result = context.get_buffer()
        for row in result:
            id_result, record_result = row
            self.assertEqual(id_result, terra.END)
            self.assertEqual(record_result, terra.END)

    def test_loadfeatureinlayer_window_defined(self):
        id_ = "identifier"
        print(self.geometries)
        record = {"a": "b", "c": "d", "geom": self.geometries["layer1"]}
        with BufferingNodeExecutionContext(
            terra.LoadFeatureInLayer(window_length=1, layer_name=self.layers[0].name)
        ) as context:
            context.write_sync((id_, record))

        result = context.get_buffer()
        for row in result:
            id_result, record_result = row
            self.assertEqual(id_result, id_)
            self.assertEqual(record_result, record)

    def test_getfeatureobject(self):
        record = {
            "geom": "value_geom",
            "key_2": "value_2",
        }
        identifier = "identifier"
        loadfeaturelayer = terra.LoadFeatureInLayer()
        loadfeaturelayer.write_layer = self.layers[0]
        result = loadfeaturelayer._get_feature_object(identifier, record)
        self.assertIsInstance(result, terra.Feature)

    def test_extractfeatures(self):
        queryset = terra.Feature.objects.all()
        extractfeature = terra.ExtractFeatures(queryset=queryset)
        extractfeature.batch_size = 1
        result = [row for row in extractfeature()]
        for row in result:
            attr_result, properties_result = row
            self.assertIsInstance(attr_result, str)
            self.assertIsInstance(properties_result, dict)

    def test_booleanintersect_exception(self):
        property_ = "property"
        booleanintersect = terra.BooleanIntersect(
            layer=next(iter(self.geometries.keys())), property=property_
        )
        record = {"key_example": "value_example"}
        with self.assertLogs():
            next(booleanintersect(identifier="identifier", record=record))
            self.assertEqual(record[property_], False)

    def test_booleanintersect_valid(self):
        property_ = "property123"
        identifier = "id"

        record = {
            "geom": next(iter(self.geometries.values())),
        }

        booleanintersect = terra.BooleanIntersect(
            layer=next(iter(self.geometries.keys())), property=property_
        )
        id_result, record_result = next(
            booleanintersect(identifier=identifier, record=record)
        )
        self.assertEqual(identifier, id_result)
        self.assertTrue(record_result[property_])

    def test_intersectionpercentbyarea_exception(self):
        layer = next(iter(self.geometries.keys()))
        property_ = "property"
        record = {"property": "value_example"}
        intersectionpercentbyarea = terra.IntersectionPercentByArea(
            layer=layer, property=property_
        )
        with self.assertLogs():
            next(intersectionpercentbyarea(identifier="identifier", record=record))

    def test_intersectionpercentbyarea_valid(self):
        property_ = "property"
        geom = "geom"
        identifier = "id"
        geom_1 = self.geometries["layerpolygon"]
        record = {
            geom: geom_1,
        }

        intersectionpercentbyarea = terra.IntersectionPercentByArea(
            layer="layerpolygon", property=property_, geom=geom
        )

        id_result, record_result = next(
            intersectionpercentbyarea(identifier=identifier, record=record)
        )

        self.assertEqual(identifier, id_result)
        self.assertIsInstance(record_result[property_], float)

    def test_closestfeatures_attribute_valid(self):
        identifier = "id"
        properties = {
            "geom": self.geometries["layerpolygon"],
        }
        closestfeatures = terra.ClosestFeatures(layer="layerpolygon", max_distance=5)
        id_result, property_result = closestfeatures(
            identifier=identifier, properties=properties
        )

        self.assertEqual(identifier, id_result)
        self.assertIsInstance(property_result["closests"], list)

    def test_closestfeatures_attribute_error(self):
        identifier = "id"
        properties = {
            "geom": self.geometries["layerpolygon"],
        }
        property_filter = {1: "error"}

        closestsfeatures = terra.ClosestFeatures(
            layer="layerpolygon", property_filter=property_filter
        )
        with mock.patch.object(
            terra.Layer.objects, "get", side_effect=AttributeError("foo")
        ):
            id_result, properties_result = closestsfeatures(
                identifier=identifier, properties=properties
            )
            self.assertEqual(identifier, id_result)
            self.assertNotIn("closests", properties_result)
            self.assertEqual(properties_result, properties)

    def test_transittimeonetomany_exception(self):
        transittimeonetomany = terra.TransitTimeOneToMany()
        identifier = "id"
        properties = {
            "points": [self.geometries["layer1"], self.geometries["layer2"]],
            "geom": self.geometries["layerpolygon"],
        }

        request = requests.Session()
        with mock.patch.object(
            request, "get", return_value=mock.Mock(ok=True)
        ) as mock_get:
            mock_get.return_value.json = mock.MagicMock(
                side_effect=JSONDecodeError("test", "test2", 123)
            )
            id_result, properties_result = transittimeonetomany(
                identifier, properties, request
            )
            self.assertEqual(id_result, identifier)
            for row in properties_result["times"]:
                self.assertEqual(row, [None])

    def test_transittimeonetomany_valid(self):
        transittimeonetomany = terra.TransitTimeOneToMany()
        identifier = "id"
        properties = {
            "points": [self.geometries["layer1"], self.geometries["layer2"]],
            "geom": self.geometries["layerpolygon"],
        }
        request = requests.Session()
        with mock.patch.object(
            request, "get", return_value=mock.Mock(ok=True)
        ) as mock_get:
            mock_get.return_value.json.return_value = {"paths": [{"fastest": 5}]}
            id_result, properties_result = transittimeonetomany(
                identifier, properties, request
            )

            self.assertEqual(id_result, identifier)
            for row in properties_result["times"]:
                self.assertIsInstance(row, list)

    def test_transittimeonetoone_else(self):
        identifier = "id"
        properties = {
            "points": [],
            "geom": self.geometries["layerpolygon"],
        }
        request = requests.Session()
        transittimeonetoone = terra.TransitTimeOneToOne()
        with mock.patch.object(
            request, "get", return_value=mock.Mock(ok=True)
        ) as mock_get:
            mock_get.return_value.json.return_value = {"paths": [{"fastest": 5}]}
            id_result, properties_result = transittimeonetoone(
                identifier=identifier, properties=properties, http=request
            )
            self.assertEqual(id_result, identifier)
            self.assertEqual(properties_result["times"], None)

    def test_transittimeonetoone_if(self):
        identifier = "id"
        properties = {
            "points": [self.geometries["layer1"], self.geometries["layer2"]],
            "geom": self.geometries["layerpolygon"],
        }
        request = requests.Session()
        transittimeonetoone = terra.TransitTimeOneToOne()
        with mock.patch.object(
            request, "get", return_value=mock.Mock(ok=True)
        ) as mock_get:
            mock_get.return_value.json.return_value = {"paths": [{"fastest": 5}]}
            id_result, properties_result = transittimeonetoone(
                identifier=identifier, properties=properties, http=request
            )

            self.assertEqual(id_result, identifier)
            self.assertIsInstance(properties_result, dict)
            self.assertIn("times", properties_result)
            self.assertIn("geom", properties_result)

    def test_accessibilityratiobytime_if(self):
        time_limits = [1, 2, 3]
        property_ = "property"
        identifier = "identifier"
        properties = {"times": False}

        accessibilityratiobytime = terra.AccessibilityRatioByTime(
            time_limits=time_limits, property=property_
        )
        id_result, properties_result = accessibilityratiobytime(
            identifier=identifier, properties=properties
        )

        self.assertEqual(id_result, identifier)
        self.assertIsInstance(properties_result, dict)
        self.assertNotIn("times", properties_result)

    def test_accessibilityratiobytime(self):
        time_limits = [11, 22, 33, 44, 55]
        property_ = "property"
        identifier = "identifier"
        properties = {
            "times": [
                [111, 222, 333, 444, 555],
                [111, 222, 333, 444, 555],
                [111, 222, 333, 444, 555],
                [111, 222, 333, 444, 555],
                [111, 222, 333, 444, 555],
            ],
            "example_key": "example_value",
            "example_key_2": "example_value_2",
            "example_key_3": "example_value_3",
            "example_key_4": "example_value_4",
            "example_key_5": "example_value_5",
        }

        accessibilityratiobytime = terra.AccessibilityRatioByTime(
            time_limits=time_limits, property=property_
        )
        id_result, properties_result = accessibilityratiobytime(
            identifier=identifier, properties=properties
        )

        self.assertEqual(id_result, identifier)
        self.assertIsInstance(properties_result[property_], float)

    def test_simplifygeom(self):
        tolerance = 5
        geom_in = "geom_in"
        geom_out = "geom_out"
        identifier = "identifier"
        record = {geom_in: next(iter(self.geometries.values()))}
        geom_type = record[geom_in].geom_type
        simplifygeom = terra.SimplifyGeom(
            tolerance=tolerance, geom_in=geom_in, geom_out=geom_out
        )
        id_result, record_result = simplifygeom(identifier=identifier, record=record)

        self.assertEqual(id_result, identifier)
        self.assertEqual(record_result[geom_out].geom_type, geom_type)
        self.assertIn(geom_in, record_result)

    def test_transformgeom(self):
        ct = 2154
        geom_in = "geom_in"
        geom_out = "geom_out"
        identifier = "identifier"
        record = {geom_in: self.geometries["layerpolygon"]}
        geom_type = record[geom_in].geom_type
        transformgeom = terra.TransformGeom(ct=ct, geom_in=geom_in, geom_out=geom_out)
        id_result, record_result = transformgeom(identifier=identifier, record=record)

        self.assertEqual(id_result, identifier)
        self.assertEqual(record_result[geom_out].geom_type, geom_type)
        self.assertIn(geom_in, record_result)

    def test_cleanolderthan(self):
        time = timezone.now()
        identifier = "identifier"
        properties = {"exemple_key": "exemple_value"}
        with BufferingNodeExecutionContext(
            terra.CleanOlderThan(time=time, layer_name=self.layers[0].name)
        ) as context:
            context.write_sync((identifier, properties))
        result = context.get_buffer()
        for row in result:
            id_result, properties_result = row
            self.assertEqual(id_result, identifier)
            self.assertEqual(properties_result, properties)
