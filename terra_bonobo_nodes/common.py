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
from django.contrib.gis.geos import GEOSGeometry, LineString, Point, Polygon  # noqa
from django.contrib.gis.geos.prototypes.io import wkt_w
from django.contrib.postgres.aggregates import ArrayAgg
from django.db.models import Count, Sum
from requests.compat import urljoin

from .db import KeyFloatTransform

logger = logging.getLogger(__name__)


class CsvDictReader(Configurable):
    """
    Extract lines from a CSV file. The file must be a BytesIO compatible object.

    Options:
      All options from csv.DictReader class are available and passed as this to the
      class creator.

    Return:
      dict(row)
    """

    delimiter = Option(str, default=csv.excel.delimiter, required=False)
    quotechar = Option(str, default=csv.excel.quotechar, required=False)
    escapechar = Option(str, default=csv.excel.escapechar, required=False)
    doublequote = Option(str, default=csv.excel.doublequote, required=False)
    skipinitialspace = Option(str, default=csv.excel.skipinitialspace, required=False)
    lineterminator = Option(str, default=csv.excel.lineterminator, required=False)
    quoting = Option(int, default=csv.excel.quoting, required=False)
    encoding = Option(str, default="utf-8", required=False)

    def get_dialect_kwargs(self):
        return {
            "delimiter": self.delimiter,
            "quotechar": self.quotechar,
            "escapechar": self.escapechar,
            "doublequote": self.doublequote,
            "skipinitialspace": self.skipinitialspace,
            "lineterminator": self.lineterminator,
            "quoting": self.quoting,
        }

    def __call__(self, content):
        if isinstance(content, (bytes, bytearray)):
            content = content.decode(self.encoding)

        reader = csv.DictReader(io.StringIO(content), **self.get_dialect_kwargs())
        for row in reader:
            yield row


class GeojsonReader(Configurable):
    """
    Extract features and its properties from a GeoJSON string.
    The input must be a valid geojson string.

    Options:
      `geom` set the dict key where the geometry will be inserted
      `allowed_projection` defines list of accepted projections

    Return:
      dict(properties)
    """

    DEFAULT_ACCEPTED_PROJECTIONS = [
        "urn:ogc:def:crs:OGC:1.3:CRS84",
        "EPSG:4326",
    ]
    geom = Option(str, required=True, positional=True)
    allowed_projection = Option(
        list, required=False, default=DEFAULT_ACCEPTED_PROJECTIONS
    )

    def __call__(self, raw_geojson_str):
        geojson = json.loads(raw_geojson_str)
        projection = geojson.get("crs", {}).get("properties", {}).get("name", None)
        if projection and projection not in self.allowed_projection:
            raise ValueError(
                f"GeoJSON projection {projection} must be in {self.allowed_projection}"
            )

        for feature in geojson.get("features", []):
            properties = feature.get("properties", {})
            properties[self.geom] = GEOSGeometry(json.dumps(feature.get("geometry")))
            yield properties


# Identifier


class IdentifierFromProperty(Configurable):
    """
    Use a property's value to set it as the identifier of the item.

    Options:
      `property` key of the property

    Return:
      list(identifier, record)
    """

    property = Option(str, required=True, positional=True)

    def __call__(self, record):
        return record[self.property], record


class GenerateIdentifier(Configurable):
    """
    Generate an identifier through a lambda functions. This allow use a method that
    generated an identifier. By defaults it returns an uuid4.

    Options:
      `generator` Lambda function that returns the generated identifier.

    Return:
      list(identifier, record)
    """

    generator = Option(
        None, required=True, default=lambda: (lambda *args: uuid.uuid4())
    )

    def __call__(self, *args):
        if not callable(self.generator):
            raise ValueError(f"Generator {self.generator} must be a callable")
        else:
            try:
                self.generator(*args)
            except TypeError:
                raise ValueError(f"Arguments not valid with {self.generator}")

        return self.generator(*args), args[-1]


# Attribute


class ExcludeAttributes(Configurable):
    """
    Pop a list of attributes from the record.

    Options:
      `excluded` list of excluded properties

    Return:
      list(identifier, record)
    """

    excluded = Option(list, required=True, positional=True)

    def __call__(self, identifier, record):
        for k in self.excluded:
            if k in record:
                record.pop(k)

        yield identifier, record


class FilterAttributes(Configurable):
    """
    Filter attributes from a record from a attributes whitelist.

    Options:
      `included` list of allowed properties

    Return:
      list(identifier, record)
    """

    included = Option(list, required=True, positional=True)

    def __call__(self, identifier, record):
        record = dict(filter(lambda kv: kv[0] in self.included, record.items()))
        yield identifier, record


class FilterByProperties(Configurable):
    """
    Filter attribute from a function.

    Options:
      `keep_eval_function` function to execute

    Return:
      list(identifier, record)
    """

    keep_eval_function = Option(None, required=True, positional=True)

    def __call__(self, identifier, record):
        if self.keep_eval_function(identifier, record):
            yield identifier, record


class MinArrayAttribute(Configurable):
    """
    Fetch the min value of the array.

    Options:
      `property_min` to get the min value

    Return:
      list(identifier, record)
    """

    property_min = Option(str, required=True, positional=True)

    def __call__(self, identifier, record):
        record[self.property_min] = min(record[self.property_min])
        return identifier, record


# Geometry


class CollectAndSum(Configurable):
    """
    Sum fields content from a features queryset.

    Options:
      `geom` the geometry field from features
      `sum_fields` fields to Sum()

    Return:
      list(identifier, record)
    """

    geom = Option(str, required=True, positional=True)
    sum_fields = Option(dict, default={}, positional=True)

    def __call__(self, identifier, features, *args, **kwargs):

        aggregates = {
            self.geom: Collect(self.geom),
            "ids": ArrayAgg("id", distinct=True),
            "point_count": Count("id"),
            **{
                k: Sum(KeyFloatTransform(field, "properties"))
                for k, field in self.sum_fields.items()
            },
        }

        yield identifier, features.aggregate(**aggregates)


class MapProperties(Configurable):
    """
    Run method on properties. Can be used to run a map() method.

    Options:
      `map_function` the function that will be executed on record

    Return:
      list(identifier, record)
    """

    map_function = Option(None, required=True, positional=True)

    def __call__(self, identifier, record):
        yield identifier, self.map_function(record)


class AttributeToGeometry(Configurable):
    """
    Pop an an attribute and transform to a GEOSGeometry object, that can be
    manipulated as a real geometry.

    Options:
      `attribute` Attribute containing the geometry
      `geom` The destination geom field

    Return:
      list(identifier, record)
    """

    attribute = Option(str, required=True)
    geom = Option(str, required=True)

    def __call__(self, identifier, record):
        record[self.geom] = self.get_geosgeometry(record.pop(self.attribute))
        yield identifier, record

    def get_geosgeometry(self, attribute):
        geom = GEOSGeometry(attribute)
        if geom.geom_type in ["Polygon", "MultiPolygon"]:
            geom = geom.buffer(0)
        elif geom.geom_type in ["LineString", "MultiLineString"]:
            geom = geom.simplify(0)
        return geom


class AttributesToPointGeometry(Configurable):
    """
    Pop x and y attributes from a record and created a Point() object.

    Options:
      `x` x attribute from record
      `y` y attribute from record
      `geom` destination attribute to the point geometry
      `srid` coordinates projection

    Return:
      list(identifier, record)
    """

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
        return identifier, record


class GeometryToJson(Configurable):
    """
    Transform a GEOSGeometry object from record to a json object which can be simplified.

    Options:
      `source` source record attribute.
      `destination` destination attribute where the json will be set.
      `simplify` simplification factor from 0.0 to 1.0.

    Return:
      list(identifier, record)
    """

    source = Option(str, required=True, positional=True)
    destination = Option(str, required=True, positional=True)
    simplify = Option(float, required=True, positional=True, default=0.0)

    def __call__(self, identifier, properties, *args, **kwargs):
        properties[self.destination] = json.loads(
            properties[self.source].simplify(self.simplify).geojson
        )
        return identifier, properties


class GeometryToCentroid(Configurable):
    """
    Get a geometry centroid an put it in a record attribute.

    Options:
      `geom` geom field where the original geometry is.
      `geom_dest` destination attribute when the centroid will be placed.

    Return:
      list(identifier, record)
    """

    geom = Option(str, required=True, positional=True)
    geom_dest = Option(str, required=True, positional=True)

    def __call__(self, identifier, properties, *args, **kwargs):
        properties[self.geom_dest] = properties[self.geom].centroid
        return identifier, properties


class Geometry3Dto2D(Configurable):
    """
    Ensure a geometry is 2D.

    Options:
      `geom` geom field where the original geometry is.
      `geom_dest` destination attribute when the 2D geometry will be placed.

    Return:
      list(identifier, record)
    """

    geom = Option(str, required=True, positional=True)
    geom_dest = Option(str, required=True, positional=True)

    def __call__(self, identifier, properties, *args, **kwargs):
        geom = properties[self.geom]
        wkt = wkt_w(dim=2).write(geom).decode()
        properties[self.geom_dest] = GEOSGeometry(wkt, srid=geom.srid)
        yield identifier, properties


# Helpers


class CopyOnPipelineSplit(Configurable):
    """
    Deep copy identifier and record objects.

    Return:
      list(identifier, record)
    """

    def __call__(self, identifier, properties):
        yield deepcopy(identifier), deepcopy(properties)


class DropIdentifier(Configurable):
    """
    Drop identifier from returned elements. Then only one element is
    in returned list.

    Return:
      record
    """

    def __call__(self, identifier, properties):
        yield properties


class DjangoLog(Configurable):
    """
    Logs the geometry through logging module.

    Options:
      `log_level` logging level

    Return:
      identifier, record
    """

    log_level = Option(int, required=False, default=logging.INFO)

    def __call__(self, identifier, record):
        logger.log(self.log_level, f"{identifier}: {record}")
        if record.get("geom"):
            logger.log(self.log_level, f'{record["geom"].ewkt}')
        return identifier, record


class IsochroneCalculation(Configurable):
    """
    Calculate the isochrone from a geometry.using graphhopper service.
    The Graphhopper endpoint is taken from django's settings.GRAPHHOPER attribute.
    Most of options attributes are from Graphhopper API, look at its API
    documentation. https://docs.graphhopper.com/#operation/getIsochrone

    Services:
      `http` requests.Session's object

    Options:
      `geom` geometry from which isochrone must be calculated
      `time_limit` Time limit
      `distance_limit` Maximum distance
      `buckets` Number of isochrone zones
      `vehicle` Kind of used vahicle (car, bike, hike, …)
      `reverse_flow` The orientation of the flow (from point to polygon, or polygon to point)

    Return:
      identifier, record
    """

    geom = Option(str, positional=True, default="geom")
    time_limit = Option(int, positional=True, default=600)
    distance_limit = Option(int, positional=True, default=0)
    buckets = Option(int, positional=True, default=3)
    vehicle = Option(str, positional=True, default="car")
    reverse_flow = Option(bool, positional=True, default=False)

    http = Service("http")

    def __call__(self, identifier, properties, http, *args, **kwargs):
        point = properties[self.geom]

        payload = {
            "time_limit": self.time_limit,
            "distance_limit": self.distance_limit,
            "buckets": self.buckets,
            "vehicle": self.vehicle,
            "reverse_flow": self.reverse_flow,
            "point": f"{point.y},{point.x}",
        }

        isochrone_url = urljoin(settings.GRAPHHOPPER, "isochrone")
        response = http.get(isochrone_url, params=payload)

        try:
            response = response.json()
            for isochrone in response.get("polygons", []):
                properties = {
                    self.geom: GEOSGeometry(json.dumps(isochrone.get("geometry"))),
                    **isochrone.get("properties", {}),
                }

                yield identifier, properties

        except json.JSONDecodeError:
            logger.error(f"Error decoding isochrone response {response.content}")


class IsochroneSubstraction(Configurable):
    geom = Option(str, positional=True, default="geom")

    @ContextProcessor
    def last(self, context, *args, **kwargs):
        yield ValueHolder(GEOSGeometry("POINT EMPTY"))

    def __call__(self, last, identifier, properties, *args, **kwargs):
        geom = properties[self.geom]

        properties[self.geom] = geom.difference(last.get())
        last.set(geom)

        yield identifier, properties


class UnionOnProperty(Configurable):
    """
    Make a Geometry union of all records

    Options:
      `geom` attribute where the final geometry will be set
      `property` property where the geometry to union is

    Return:
      identifier, record
    """

    geom = Option(str, positional=True, default="geom")
    property = Option(str, positional=True, required=True)

    @ContextProcessor
    def buffer(self, context, *args, **kwargs):
        buffer = yield ValueHolder({})

        for level, geometry in buffer.get().items():
            context.send(level, {"level": level, self.geom: geometry})

    def __call__(self, buffer, identifier, properties, *args, **kwargs):

        key = properties.get(self.property)
        unions = buffer.get()
        unions.get(key)
        unions[key] = unions.get(key, GEOSGeometry("POINT EMPTY")) | properties.get(
            self.geom, GEOSGeometry("POINT EMPTY")
        )
