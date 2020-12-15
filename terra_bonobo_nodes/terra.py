import logging
from copy import deepcopy
from json import JSONDecodeError

from bonobo.config import Configurable, Option, Service
from bonobo.config.processors import ContextProcessor
from bonobo.constants import END, NOT_MODIFIED
from bonobo.util.objects import ValueHolder
from django.conf import settings
from django.contrib.gis.db.models import Union
from django.contrib.gis.db.models.functions import (
    Distance,
    Intersection,
    MakeValid,
    Transform,
)
from django.contrib.gis.geos import GEOSGeometry
from django.db import connection, transaction
from geostore.models import Feature, FeatureQuerySet, Layer  # noqa
from requests.compat import urljoin

logger = logging.getLogger(__name__)

GEOS_EMPTY_POINT = GEOSGeometry("POINT EMPTY")


class LayerClusters(Configurable):
    """
    Extract cluster from layers


    Options:
      `input_layers` list of input layers
      `metric_projection_srid` used projection
      `distance` minimal distance between each cluster

    Return:
      Point cluster point object
      QuerySet QuerySet of all features included in the cluster
    """

    input_layers = Option(list, positional=True, required=True)
    metric_projection_srid = Option(int, positional=True, required=True)
    distance = Option(int, positional=True, required=True)

    def __call__(self, *args, **kwargs):
        args = [
            self.metric_projection_srid,
            self.distance,
            [input_layer.pk for input_layer in self.input_layers],
        ]

        with connection.cursor() as cursor:
            sql_query = f"""
                SELECT
                    array_agg(id) AS ids,
                    ST_AsText(ST_SnapToGrid(ST_Transform(geom, %s), %s)) AS cluster_id
                FROM
                    {Feature._meta.db_table}
                WHERE
                    layer_id = ANY(%s::INT[])
                GROUP BY
                    cluster_id
            """

            cursor.execute(sql_query, args)
            for features, cluster in cursor.fetchall():
                yield cluster, Feature.objects.filter(pk__in=features)


class SubdivideGeom(Configurable):
    """
    Execute ST_Subdivide to an input geometry


    Options:
      `max_vertices` numbe maximal of vertices of the new geometry
      `geom` geom field where is located the geometry

    Return:
      identifier identifier of the new record
      record properties of the record
    """

    max_vertices = Option(int, positional=True, default=256)
    geom = Option(str, positional=True, default="geom")

    def __call__(self, identifier, properties, *args, **kwargs):
        args = [
            properties[self.geom].ewkt,
            self.max_vertices,
        ]

        with connection.cursor() as cursor:
            sql_query = (
                "SELECT ST_Subdivide(ST_Buffer(ST_GeomFromText(%s), 0), %s) AS geom"
            )
            cursor.execute(sql_query, args)
            id = 0
            for (geom,) in cursor.fetchall():
                properties = deepcopy(properties)
                properties[self.geom] = GEOSGeometry(geom)
                yield f"{identifier}-{id}", properties
                id += 1


class LoadFeatureInLayer(Configurable):
    """
    Load feature data in input layer

    Options:
      `geom` geom field where is located the geometry
      `layer` layer where to insert the geometry and its attributes
      `window_length` size of bulk import

    Services:
      `service_layer` Layer where to insert geometries, used if layer argument is empty

    Return:
      NOT_MODIFIED
    """

    geom = Option(str, positional=True, default="geom")
    layer = Option(None, required=False, positional=True)
    window_length = Option(int, default=200)

    service_layer = Service("output_layer")

    @ContextProcessor
    def buffer(self, context, *args, service_layer, **kwargs):
        buffer = yield ValueHolder([])

        if len(buffer):
            # Final call if there is content in buffer
            self.__call__(buffer, END, END, service_layer)

    def __call__(self, buffer, identifier, record, service_layer, *args, **kwargs):
        self.write_layer = self.layer if self.layer else service_layer

        is_final = identifier == END and record == END

        if not is_final:
            buffer.append(
                (
                    identifier,
                    record,
                )
            )

        if len(buffer) >= self.window_length or is_final:
            with transaction.atomic(savepoint=False):
                Feature.objects.filter(
                    layer=self.write_layer, identifier__in=[i for i, r in buffer]
                ).delete()
                Feature.objects.bulk_create(
                    [self._get_feature_object(*feature) for feature in buffer]
                )
            buffer.set([])
            return NOT_MODIFIED

    def _get_feature_object(self, identifier, record):
        properties = record.copy()
        geometry = properties.pop(self.geom, GEOS_EMPTY_POINT)

        return Feature(
            layer=self.write_layer,
            identifier=identifier,
            geom=geometry,
            properties=properties,
        )


class ExtractFeatures(Configurable):
    """
    Extract features from a queryset

    Options:
      `queryset` Feature QuerySet containing geometries and attributes
      `id_field` field containing the identifier
      `extra_properties` dict of extra attributes extracted from the feature

    Return:
      str identifier of the record using id_field
      dict record
    """

    queryset = Option(None, required=True, positional=True)
    id_field = Option(str, required=True, positional=True, default="identifier")
    extra_properties = Option(dict, required=True, positional=True, default={})
    batch_size = 1000

    def __call__(self, *args, **kwargs):

        count = self.queryset.count()

        for start in range(0, count, self.batch_size):
            end = min(start + self.batch_size, count)
            features = self.queryset[start:end]
            for feature in features:
                properties = {
                    **feature.properties,
                    **{
                        attribute: getattr(feature, field)
                        for attribute, field in self.extra_properties.items()
                    },
                }
                yield getattr(feature, self.id_field), properties


class BooleanIntersect(Configurable):
    """
    Intersect geometry witch all geometries of one layer

    Options:
      `layer` Layer to intersect
      `property` property where to put the resulted boolean
      `geom` geometry attribute in record

    Return:
      str identifier of the record
      dict record updated
    """

    layer = Option(str, required=True, positional=True)
    property = Option(str, required=True, positional=True)
    geom = Option(str, positional=True, default="geom")

    def __call__(self, identifier, record, *args, **kwargs):
        layer = Layer.objects.get(name=self.layer)
        try:
            record[self.property] = layer.features.filter(
                geom__intersects=record[self.geom]
            ).exists()
        except Exception as e:
            record[self.property] = False
            logger.error(f"An error occured doing BooleanIntersect: {e}")

        yield identifier, record


class IntersectionPercentByArea(Configurable):
    """
    Get percentage of intersection of a geometry

    Options:
      `layer` Layer to intersect
      `property` property where to put the resulted intersection
      `geom` geometry attribute in record

    Return:
      str identifier of the record
      dict record updated
    """

    layer = Option(str, required=True, positional=True)
    property = Option(str, required=True, positional=True)
    geom = Option(str, positional=True, default="geom")

    def __call__(self, identifier, record, *args, **kwargs):
        layer = Layer.objects.get(name=self.layer)
        try:
            zone = (
                layer.features.filter(geom__intersects=record[self.geom])
                .annotate(
                    intersection=MakeValid(Intersection("geom", record[self.geom]))
                )
                .aggregate(zone=Union("intersection"))["zone"]
            )

            record[self.property] = zone and zone.area / record[self.geom].area or 0.0

        except Exception as e:
            logger.error(f"identifier {identifier} got error {e}")

        yield identifier, record


class ClosestFeatures(Configurable):
    """
    Get closes features of the geometry in a layer

    Options:
      `layer` Layer to intersect
      `property_filter` dict of properties to filter in layer's features
      `geom` geometry attribute in record
      `closests` property where to put closest features
      `limit` number of features maximum to load
      `max_distance` maximal distance from original geometry

    Return:
      str identifier of the record
      dict record updated
    """

    layer = Option(str, positional=True, required=True)
    property_filter = Option(dict, default={})
    geom = Option(str, default="geom")
    closests = Option(str, default="closests")
    limit = Option(int, default=1)
    max_distance = Option(int, default=-1)

    def __call__(self, identifier, properties, *args, **kwargs):
        geom_point = properties[self.geom].centroid
        properties_filters = {
            f"properties__{k}": v for k, v in self.property_filter.items()
        }

        try:
            closest_points = (
                Layer.objects.get(name=self.layer)
                .features.filter(**properties_filters)
                .exclude(geom=GEOSGeometry("POINT EMPTY"))
                .annotate(
                    distance=Distance(
                        Transform("geom", 4326), Transform(geom_point, 4326)
                    )
                )
            )
            if self.max_distance > 0:
                closest_points = closest_points.filter(distance__lt=self.max_distance)
            closest_points = closest_points.order_by("distance")[: self.limit]
            properties[self.closests] = properties.get(self.closests, []) + [
                c.geom for c in closest_points
            ]
            return identifier, properties
        except AttributeError:
            return identifier, properties


class TransitTimeOneToMany(Configurable):
    """
    Calculate transit time from geometry to list of points.
    Settings can be found in graphhopper API documentation.

    Options:
      `vehicules` vehicules to use (car, bike, hike, …)
      `weighting` what kind of way (default: fastest)
      `elevation` take care of terrain elevation
      `geom` where is the original geometry
      `points` destination points to calculate
      `times_property` where to insert calculated times

    Services:
      `http` requests.Session's object

    Return:
      str identifier of the record
      dict record updated
    """

    vehicles = Option(list, positional=True, default=["car"])
    weighting = Option(str, positional=True, default="fastest")
    elevation = Option(bool, positional=True, default=False)
    geom = Option(str, positional=True, default="geom")
    points = Option(str, positional=True, default="points")
    times_property = Option(str, positional=True, default="times")

    http = Service("http")

    def __call__(self, identifier, properties, http, *args, **kwargs):
        end_point = properties[self.geom].centroid
        # Starts from point to deals with oneway motorway
        points = properties.pop(self.points)
        dim = "time" if self.weighting == "fastest" else "distance"

        times = []
        for point in points:
            time = []
            for vehicle in self.vehicles:
                routing_url = urljoin(settings.GRAPHHOPPER, "route")
                payload = {
                    "point": [f"{point.y},{point.x}", f"{end_point.y},{end_point.x}"],
                    "vehicle": vehicle,
                    "weighting": self.weighting,
                    "elevation": self.elevation,
                    "instructions": False,
                    "calc_points": False,
                }

                response = http.get(routing_url, params=payload)

                try:
                    response = response.json()
                    time += [response.get("paths", [])[0].get(dim)]
                except (IndexError, JSONDecodeError):
                    time += [None]
            times += [time]

        properties[self.times_property] = times
        return identifier, properties


class TransitTimeOneToOne(TransitTimeOneToMany):
    """
    Same as TransitTimeOneToMany but for only one destination. Uses the same API.
    """

    def __call__(self, *args, **kwargs):
        identifier, properties = super().__call__(*args, **kwargs)
        if properties[self.times_property]:
            properties[self.times_property] = properties[self.times_property][0][0]
        else:
            properties[self.times_property] = None

        return identifier, properties


class AccessibilityRatioByTime(Configurable):
    """
    Calculate accesibility using transit times

    Options:
      `time_limits` dict of time limits by type of vehicle
      `property` property where to set in the record the resulted ratio
      `times` where are the transit time stored in original record

    Return:
      str identifier of the record
      dict record updated
    """

    time_limits = Option(list, positional=True, required=True)
    property = Option(str, positional=True, required=True)
    times = Option(str, positional=True, default="times")

    def __call__(self, identifier, properties, *args, **kwargs):
        transit_times = properties.pop(self.times)

        if not transit_times:
            return identifier, properties
        else:
            n_points = len(transit_times)

            access = [False] * n_points
            for n in range(0, n_points):
                for mode_i, limit in enumerate(self.time_limits):
                    time = transit_times[n][mode_i]
                    access[n] = access[n] or time is not None and time <= limit

            properties[self.property] = (
                sum(access[n] for n in range(0, n_points)) / n_points
            )
            return identifier, properties


class SimplifyGeom(Configurable):
    """
    Simplify a geometry


    Options:
      `tolerance` tolerance of simplification
      `geom_in` property of input geometry
      `geom_out` property of output geometry

    Return:
      str identifier of the record
      dict record updated
    """

    tolerance = Option(int, positional=True, required=True)
    geom_in = Option(str, positional=True, default="geom")
    geom_out = Option(str, positional=True, default="geom")

    def __call__(self, identifier, record, *args, **kwargs):
        record[self.geom_out] = record[self.geom_in].simplify(self.tolerance)
        return identifier, record


class TransformGeom(Configurable):
    """
    Transform geometry

    Options:
      `ct` destination projection
      `geom_in` property of input geometry
      `geom_out` property of output geometry

    Return:
      str identifier of the record
      dict record updated
    """

    ct = Option(str, positional=True, required=True)
    geom_in = Option(str, positional=True, default="geom")
    geom_out = Option(str, positional=True, default="geom")

    def __call__(self, identifier, record, *args, **kwargs):
        record[self.geom_out] = record[self.geom_in].transform(self.ct, clone=True)
        return identifier, record


class CleanOlderThan(Configurable):
    """
    Clean features of layer older than input date

    Options:
      `time` date threshold

    Return:
      str identifier of the record
      dict record updated
    """

    time = Option(None, required=True, positional=True)

    output_layer = Service("output_layer")

    @ContextProcessor
    def context(self, context, output_layer, *args, **kwargs):
        yield context
        layer = context.get_service("output_layer")
        layer.features.filter(updated_at__lt=self.time).delete()

    def __call__(self, context, identifier, properties, output_layer, *args, **kwargs):
        return NOT_MODIFIED


class IntersectionGeom(Configurable):
    """
    Cut original geometry with intersection of layers geometries

    Options:
      `layer` layer to intersect
      `geom` property of input geometry
      `geom_dest` property of output geometry

    Return:
      str identifier of the record
      dict record updated
    """

    layer = Option(str, required=True, positional=True)
    geom = Option(str, positional=True, default="geom")
    geom_dest = Option(str, positional=True, default="geom")

    def __call__(self, identifier, record, *args, **kwargs):
        layer = Layer.objects.get(name=self.layer)
        try:
            zone = (
                layer.features.filter(geom__intersects=record[self.geom])
                .annotate(
                    intersection=MakeValid(Intersection("geom", record[self.geom]))
                )
                .aggregate(zone=Union("intersection"))["zone"]
            )

            record[self.geom_dest] = zone

        except Exception as e:
            logger.error(f"identifier {identifier} got error {e}")

        yield identifier, record
