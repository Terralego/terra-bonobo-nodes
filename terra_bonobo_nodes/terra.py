import logging
from copy import deepcopy
from json import JSONDecodeError

from bonobo.config import Configurable, Option, Service
from bonobo.config.processors import ContextProcessor
from bonobo.constants import END, NOT_MODIFIED
from bonobo.util.objects import ValueHolder
from django.conf import settings
from django.contrib.gis.db.models import Union
from django.contrib.gis.db.models.functions import (Distance, Intersection,
                                                    MakeValid, Transform)
from django.contrib.gis.geos import GEOSGeometry
from django.db import connection, transaction
from geostore.models import Feature, FeatureQuerySet, Layer  # noqa
from requests.compat import urljoin

logger = logging.getLogger(__name__)

GEOS_EMPTY_POINT = GEOSGeometry('POINT EMPTY')


class LayerClusters(Configurable):
    input_layers = Option(list, positional=True, required=True)
    metric_projection_srid = Option(int, positional=True, required=True)
    distance = Option(int, positional=True, required=True)

    def __call__(self, *args, **kwargs):
        args = [
            self.metric_projection_srid,
            self.distance,
            [l.pk for l in self.input_layers],
        ]

        with connection.cursor() as cursor:
            sql_query = f'''
                SELECT
                    array_agg(id) AS ids,
                    ST_AsText(ST_SnapToGrid(ST_Transform(geom, %s), %s)) AS cluster_id
                FROM
                    {Feature._meta.db_table}
                WHERE
                    layer_id = ANY(%s::INT[])
                GROUP BY
                    cluster_id
            '''

            cursor.execute(sql_query, args)
            for features, cluster in cursor.fetchall():
                yield cluster, Feature.objects.filter(pk__in=features)


class SubdivideGeom(Configurable):
    max_vertices = Option(int, positional=True, default=256)
    geom = Option(str, positional=True, default='geom')

    def __call__(self, identifier, properties, *args, **kwargs):
        args = [
            properties[self.geom].ewkt,
            self.max_vertices,
        ]

        with connection.cursor() as cursor:
            sql_query = f'''
                SELECT
                    ST_Subdivide(ST_GeomFromText(%s), %s) AS geom
            '''
            cursor.execute(sql_query, args)
            id = 0
            for geom, in cursor.fetchall():
                properties = deepcopy(properties)
                properties[self.geom] = GEOSGeometry(geom)
                yield f'{identifier}-{id}', properties
                id += 1


class LoadFeatureInLayer(Configurable):
    geom = Option(str, positional=True, default='geom')
    layer = Option(None, required=False, positional=True)
    window_length = Option(int, default=200)

    service_layer = Service('output_layer')

    @ContextProcessor
    def buffer(self, context, *args, **kwargs):
        buffer = yield ValueHolder([])

        if len(buffer):
            context.input._writable_runlevel = 1
            context.input._runlevel += 1
            context.input.put((END, END))
            context.input.put(END)
            context.step()

    def __call__(self, buffer, identifier, record, service_layer, *args, **kwargs):
        self.write_layer = self.layer if self.layer else service_layer

        is_final = identifier == END and record == END

        if not is_final:
            buffer.append((identifier, record, ))

        if len(buffer) >= self.window_length or is_final:
            with transaction.atomic(savepoint=False):
                Feature.objects.filter(identifier__in=[i for i, r in buffer]).delete()
                Feature.objects.bulk_create(
                    [self._get_feature_object(*feature) for feature in buffer]
                )
            buffer.set([])
            yield NOT_MODIFIED

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
    queryset = Option(None, required=True, positional=True)
    id_field = Option(str, required=True, positional=True, default='identifier')
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
                    }
                }
                yield getattr(feature, self.id_field), properties


class BooleanIntersect(Configurable):
    layer = Option(str, required=True, positional=True)
    property = Option(str, required=True, positional=True)
    geom = Option(str, positional=True, default='geom')

    def __call__(self, identifier, record, *args, **kwargs):
        layer = Layer.objects.get(name=self.layer)
        try:
            record[self.property] = layer.features.filter(geom__intersects=record[self.geom]).exists()
        except Exception as e:
            record[self.property] = False
            logger.error(f'An error occured doing BooleanIntersect: {e}')

        yield identifier, record


class IntersectionPercentByArea(Configurable):
    layer = Option(str, required=True, positional=True)
    property = Option(str, required=True, positional=True)
    geom = Option(str, positional=True, default='geom')

    def __call__(self, identifier, record, *args, **kwargs):
        layer = Layer.objects.get(name=self.layer)
        try:
            zone = layer.features.filter(
                    geom__intersects=record[self.geom]
                ).annotate(
                    intersection=MakeValid(Intersection('geom', record[self.geom]))
                ).aggregate(
                    zone=Union('intersection')
                )['zone']

            record[self.property] = zone and zone.area / record[self.geom].area or 0.0

        except Exception as e:
            logger.error(f'identifier {identifier} got error {e}')

        yield identifier, record


class ClosestFeatures(Configurable):
    layer = Option(str, positional=True, required=True)
    property_filter = Option(dict, default={})
    geom = Option(str, default='geom')
    closests = Option(str, default='closests')
    limit = Option(int, default=1)
    max_distance = Option(int, default=-1)

    def __call__(self, identifier, properties, *args, **kwargs):
        geom_point = properties[self.geom].centroid
        properties_filters = {f'properties__{k}': v for k, v in self.property_filter.items()}

        try:
            closest_points = Layer.objects.get(
                                name=self.layer
                            ).features.filter(
                                **properties_filters
                            ).exclude(
                                geom=GEOSGeometry('POINT EMPTY')
                            ).annotate(
                                distance=Distance(
                                    Transform('geom', 4326),
                                    Transform(geom_point, 4326)
                                )
                            )
            if self.max_distance > 0:
                closest_points = closest_points.filter(
                    distance__lt=self.max_distance
                )
            closest_points = closest_points.order_by('distance')[:self.limit]
            properties[self.closests] = (
                properties.get(self.closests, []) + [c.geom for c in closest_points]
            )
            return identifier, properties
        except AttributeError:
            return identifier, properties


class TransitTimeOneToMany(Configurable):
    vehicles = Option(list, positional=True, default=['car'])
    weighting = Option(str, positional=True, default='fastest')
    elevation = Option(bool, positional=True, default=False)
    geom = Option(str, positional=True, default='geom')
    points = Option(str, positional=True, default='points')
    times_property = Option(str, positional=True, default='times')

    http = Service('http')

    def __call__(self, identifier, properties, http, *args, **kwargs):
        start_point = properties[self.geom].centroid
        points = properties.pop(self.points)
        dim = 'time' if self.weighting == 'fastest' else 'distance'

        times = []
        for point in points:
            time = []
            for vehicle in self.vehicles:
                routing_url = urljoin(settings.GRAPHHOPPER, 'route')
                payload = {
                    'point': [f'{start_point.y},{start_point.x}', f'{point.y},{point.x}'],
                    'vehicle': vehicle,
                    'weighting': self.weighting,
                    'elevation': self.elevation,
                    'instructions': False,
                    'calc_points': False,
                }

                response = http.get(routing_url, params=payload)

                try:
                    response = response.json()
                    time += [response.get('paths', [])[0].get(dim)]
                except (IndexError, JSONDecodeError):
                    time += [None]
            times += [time]

        properties[self.times_property] = times
        return identifier, properties


class TransitTimeOneToOne(TransitTimeOneToMany):
    def __call__(self, *args, **kwargs):
        identifier, properties = super().__call__(*args, **kwargs)
        if properties[self.times_property]:
            properties[self.times_property] = properties[self.times_property][0][0]
        else:
            properties[self.times_property] = None

        return identifier, properties


class AccessibilityRatioByTime(Configurable):
    time_limits = Option(list, positional=True, required=True)
    property = Option(str, positional=True, required=True)
    times = Option(str, positional=True, default='times')

    def __call__(self, identifier, properties, *args, **kwargs):
        transit_times = properties.pop(self.times)

        if not transit_times:
            return identifier, properties
        else:
            # print("transit_times")  # noqa
            n_points = len(transit_times)

            access = [False] * n_points
            for n in range(0, n_points):
                for mode_i, limit in enumerate(self.time_limits):
                    time = transit_times[n][mode_i]
                    access[n] = access[n] or time is not None and time <= limit

            properties[self.property] = sum(access[n] for n in range(0, n_points)) / n_points
            return identifier, properties


class SimplifyGeom(Configurable):
    tolerance = Option(int, positional=True, required=True)
    geom_in = Option(str, positional=True, default='geom')
    geom_out = Option(str, positional=True, default='geom')

    def __call__(self, identifier, record, *args, **kwargs):
        record[self.geom_out] = record[self.geom_in].simplify(self.tolerance)
        return identifier, record


class TransformGeom(Configurable):
    ct = Option(str, positional=True, required=True)
    geom_in = Option(str, positional=True, default='geom')
    geom_out = Option(str, positional=True, default='geom')

    def __call__(self, identifier, record, *args, **kwargs):
        record[self.geom_out] = record[self.geom_in].transform(self.ct, clone=True)
        return identifier, record


class CleanOlderThan(Configurable):
    time = Option(None, required=True, positional=True)

    output_layer = Service('output_layer')

    @ContextProcessor
    def context(self, context, output_layer, *args, **kwargs):
        yield context
        layer = context.get_service('output_layer')
        print("Count to be deleted: " + str(layer.features.filter(updated_at__lt=self.time).count()))
        layer.features.filter(updated_at__lt=self.time).delete()

    def __call__(self, context, identifier, properties, output_layer, *args, **kwargs):
        return NOT_MODIFIED


class IntersectionGeom(Configurable):
    layer = Option(str, required=True, positional=True)
    geom = Option(str, positional=True, default='geom')
    geom_dest = Option(str, positional=True, default='geom')

    def __call__(self, identifier, record, *args, **kwargs):
        layer = Layer.objects.get(name=self.layer)
        try:
            zone = layer.features.filter(
                    geom__intersects=record[self.geom]
                ).annotate(
                    intersection=MakeValid(Intersection('geom', record[self.geom]))
                ).aggregate(
                    zone=Union('intersection')
                )['zone']

            record[self.geom_dest] = zone

        except Exception as e:
            logger.error(f'identifier {identifier} got error {e}')

        yield identifier, record
