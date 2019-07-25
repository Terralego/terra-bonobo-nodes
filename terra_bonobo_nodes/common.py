import csv
import io
import json
import logging
import uuid
from copy import deepcopy
from bonobo.config import Configurable, Option, Service
from bonobo.config.processors import ContextProcessor
from bonobo.util.objects import ValueHolder
from django.conf import settings
from django.contrib.gis.db.models import Collect
from django.contrib.gis.geos import GEOSGeometry, Point, Polygon, MultiPoint, LineString, GeometryCollection, MultiLineString
from django.contrib.gis.geos.prototypes.io import wkt_w
from django.contrib.postgres.aggregates import ArrayAgg
from django.db.models import Count, Sum
from requests.compat import urljoin
from .db import KeyFloatTransform

logger = logging.getLogger(__name__)


class CsvDictReader(Configurable):
    delimiter = Option(str, default=csv.excel.delimiter, required=False)
    quotechar = Option(str, default=csv.excel.quotechar, required=False)
    escapechar = Option(str, default=csv.excel.escapechar, required=False)
    doublequote = Option(str, default=csv.excel.doublequote, required=False)
    skipinitialspace = Option(str, default=csv.excel.skipinitialspace, required=False)
    lineterminator = Option(str, default=csv.excel.lineterminator, required=False)
    quoting = Option(int, default=csv.excel.quoting, required=False)
    encoding = Option(str, default='utf-8', required=False)

    def get_dialect_kwargs(self):
        return {
            'delimiter': self.delimiter,
            'quotechar': self.quotechar,
            'escapechar': self.escapechar,
            'doublequote': self.doublequote,
            'skipinitialspace': self.skipinitialspace,
            'lineterminator': self.lineterminator,
            'quoting': self.quoting,
        }

    def __call__(self, content):
        if isinstance(content, (bytes, bytearray)):
            content = content.decode(self.encoding)

        reader = csv.DictReader(io.StringIO(content), **self.get_dialect_kwargs())
        for row in reader:
            yield row


class GeojsonReader(Configurable):
    DEFAULT_ACCEPTED_PROJECTIONS = [
        'urn:ogc:def:crs:OGC:1.3:CRS84',
        'EPSG:4326',
    ]
    geom = Option(str, required=True, positional=True)
    allowed_projection = Option(list, required=False, default=DEFAULT_ACCEPTED_PROJECTIONS)

    def __call__(self, raw_geojson_str):
        geojson = json.loads(raw_geojson_str)
        projection = geojson.get('crs', {}).get('properties', {}).get('name', None)
        if projection and projection not in self.allowed_projection:
            raise ValueError(f'GeoJSON projection {projection} must be in {self.allowed_projection}')

        for feature in geojson.get('features', []):
            properties = feature.get('properties', {})
            properties[self.geom] = GEOSGeometry(json.dumps(feature.get('geometry')))
            yield properties

# Identifier


class IdentifierFromProperty(Configurable):
    property = Option(str, required=True, positional=True)

    def __call__(self, record):
        return record.pop(self.property), record


class GenerateIdentifier(Configurable):
    generator = Option(None, required=True, default=lambda: (lambda *args: uuid.uuid4()))

    def __call__(self, *args):
        if not callable(self.generator):
            raise ValueError(f'Generator {self.generator} must be a callable')
        else:
            try:
                self.generator(*args)
            except TypeError:
                raise ValueError(f'Arguments not valid with {self.generator}')

        return self.generator(*args), args[-1]

# Attribute


class ExcludeAttributes(Configurable):
    excluded = Option(list, required=True, positional=True)

    def __call__(self, identifier, record):
        for k in self.excluded:
            if k in record:
                record.pop(k)

        yield identifier, record


class FilterAttributes(Configurable):
    included = Option(list, required=True, positional=True)

    def __call__(self, identifier, record):
        record = dict(filter(lambda kv: kv[0] in self.included, record.items()))
        yield identifier, record


class FilterByProperties(Configurable):
    keep_eval_function = Option(None, required=True, positional=True)

    def __call__(self, identifier, record):
        if self.keep_eval_function(identifier, record):
            yield identifier, record

# Geometry


class CollectAndSum(Configurable):
    geom = Option(str, required=True, positional=True)
    sum_fields = Option(dict, default={}, positional=True)

    def __call__(self, identifier, features, *args, **kwargs):

        aggregates = {
            self.geom: Collect(self.geom),
            'ids': ArrayAgg('id', distinct=True),
            'point_count': Count('id'),
            **{k: Sum(KeyFloatTransform(field, 'properties')) for k, field in self.sum_fields.items()}
        }

        yield identifier, features.aggregate(**aggregates)


class MapProperties(Configurable):
    map_function = Option(None, required=True, positional=True)

    def __call__(self, identifier, record):
        yield identifier, self.map_function(record)


class AttributeToGeometry(Configurable):
    attribute = Option(str, required=True)
    geom = Option(str, required=True)

    def __call__(self, identifier, record):
        record[self.geom] = self.get_geosgeometry(record.pop(self.attribute))
        yield identifier, record

    def get_geosgeometry(self, attribute):
        geom = GEOSGeometry(attribute)
        if geom.geom_type in ['Polygon', 'MultiPolygon']:
            geom = geom.buffer(0)
        elif geom.geom_type in ['LineString', 'MultiLineString']:
            geom = geom.simplify(0)
        return geom


class AttributesToPointGeometry(Configurable):
    x = Option(str, required=True)
    y = Option(str, required=True)
    geom = Option(str, required=True)
    srid = Option(int, required=False, default=4326)

    def __call__(self, identifier, record):
        x = record.pop(self.x)
        y = record.pop(self.y)

        try:
            record[self.geom] = Point(float(x), float(y), srid=self.srid)
        except (TypeError, ValueError) as e:
            raise ValueError(f'Fails to cast ("{x}", "{y}") to float') from e
        yield identifier, record


class GeometryToJson(Configurable):
    source = Option(str, required=True, positional=True)
    destination = Option(str, required=True, positional=True)
    simplify = Option(float, required=True, positional=True, default=0.0)

    def __call__(self, identifier, properties, *args, **kwargs):
        properties[self.destination] = json.loads(properties[self.source].simplify(self.simplify).geojson)
        yield identifier, properties


class GeometryToCentroid(Configurable):
    geom = Option(str, required=True, positional=True)
    geom_dest = Option(str, required=True, positional=True)

    def __call__(self, identifier, properties, *args, **kwargs):
        properties[self.geom_dest] = properties[self.geom].centroid
        yield identifier, properties


class Geometry3Dto2D(Configurable):
    geom = Option(str, required=True, positional=True)
    geom_dest = Option(str, required=True, positional=True)

    def __call__(self, identifier, properties, *args, **kwargs):
        geom = properties[self.geom]
        wkt = wkt_w(dim=2).write(geom).decode()
        properties[self.geom_dest] = GEOSGeometry(wkt, srid=geom.srid)
        yield identifier, properties

# Helpers


class CopyOnPipelineSplit(Configurable):
    def __call__(self, identifier, properties):
        yield deepcopy(identifier), deepcopy(properties)


class DropIdentifier(Configurable):
    def __call__(self, identifier, properties):
        yield properties


class DjangoLog(Configurable):
    log_level = Option(int, required=False, default=logging.INFO)

    def __call__(self, identifier, record):
        logger.log(self.log_level, f'{identifier}: {record}')
        if record.get('geom'):
            logger.log(self.log_level, f'{record["geom"].ewkt}')
        return identifier, record


class IsochroneCalculation(Configurable):
    geom = Option(str, positional=True, default='geom')
    time_limit = Option(int, positional=True, default=600)
    distance_limit = Option(int, positional=True, default=0)
    buckets = Option(int, positional=True, default=3)
    vehicle = Option(str, positional=True, default='car')
    reverse_flow = Option(bool, positional=True, default=False)

    http = Service('http')

    def __call__(self, identifier, properties, http, *args, **kwargs):
        point = properties[self.geom]

        payload = {
            'time_limit': self.time_limit,
            'distance_limit': self.distance_limit,
            'buckets': self.buckets,
            'vehicle': self.vehicle,
            'reverse_flow': self.reverse_flow,
            'point': f'{point.y},{point.x}',
        }

        isochrone_url = urljoin(settings.GRAPHHOPPER, 'isochrone')
        response = http.get(isochrone_url, params=payload)

        try:
            response = response.json()
            for isochrone in response.get('polygons', []):
                properties = {
                    self.geom: GEOSGeometry(json.dumps(isochrone.get('geometry'))),
                    **isochrone.get('properties', {})
                }

                yield identifier, properties

        except json.JSONDecodeError:
            logger.error(f'Error decoding isochrone response {response.content}')


class UnionOnProperty(Configurable):
    geom = Option(str, positional=True, default='geom')
    property = Option(str, positional=True, required=True)

    @ContextProcessor
    def buffer(self, context, *args, **kwargs):
        buffer = yield ValueHolder({})

        for level, geometry in buffer.get().items():
            context.send(
                level,
                {
                    'level': level,
                    self.geom: geometry
                }
            )

    def __call__(self, buffer, identifier, properties, *args, **kwargs):

        key = properties.get(self.property)
        unions = buffer.get()
        unions.get(key)
        unions[key] = (
            unions.get(key, GEOSGeometry('POINT EMPTY'))
            | properties.get(self.geom, GEOSGeometry('POINT EMPTY'))
        )