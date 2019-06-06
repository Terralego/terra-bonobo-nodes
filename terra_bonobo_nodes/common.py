import csv
import io
import json
import logging
import uuid
from copy import deepcopy
from io import StringIO,BytesIO
from bonobo.config import Configurable, Option
from collections import OrderedDict
from django.contrib.gis.geos import GEOSGeometry, Point
from iter import irange

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


class IdentifierFromProperty(Configurable):
    property = Option(str, required=True, positional=True)

    def __call__(self, record):
        yield record.pop(self.property), record


class GenerateIdentifier(Configurable):
    generator = Option(None, required=True, default=lambda: (lambda *args: uuid.uuid4()))

    def __call__(self, *args):
        yield self.generator(*args), args[-1]


total = 0 
for i in irange(1000):
    total += i
test = GenerateIdentifier(total)

retour = [row for row in test.__call__('arg')]
print(retour)