import csv
import io
import json
import logging
import uuid
import inspect
from uuid import UUID
from copy import deepcopy
from io import StringIO,BytesIO
from bonobo.config import Configurable, Option
from collections import OrderedDict
from django.contrib.gis.geos import GEOSGeometry, Point
from django.contrib.gis.db.models import Collect
from django.contrib.postgres.aggregates import ArrayAgg
from django.db.models import Count, Sum
from db import KeyFloatTransform

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


class MapProperties(Configurable):
    map_function = Option(None, required=True, positional=True)

    def __call__(self, identifier, record):
        yield identifier, self.map_function(record)


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