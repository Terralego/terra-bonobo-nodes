import logging
import os
import subprocess
import tempfile
from enum import Enum

from bonobo.config import Configurable, Option, Service

OVERPASS_URL = "http://overpass-api.de/api/interpreter"

logger = logging.getLogger(__name__)


class OverpassExtract(Configurable):
    """
    Extract records from an overpass query

    Options:
      `query` The query to execute in the overpass service
      `overpass_url` Overpass endpoint,

    Services:
      `http` requests.Session object

    Return:
      str result of query
    """

    query = Option(str, required=True, positional=True)
    overpass_url = Option(str, required=False, positional=False, default=OVERPASS_URL)

    http = Service("http")

    def __call__(self, http):
        response = http.post(self.overpass_url, data=self.query)
        if not response.ok:
            logger.error(response.text)
            raise RuntimeError("Overpass query fails")
        yield response.content.decode("utf-8")


class OsmXMLtoGeojson(Configurable):
    """
    Transforms XML from OSM OverPass service to geojson format

    Options:
      `type_features` The type of geometries (points, lines, …)

    Return:
      str geojson data
    """

    class Geometry(Enum):
        POINTS = "points"
        LINES = "lines"
        MULTILINESTRINGs = "multilinestrings"
        MULTIPOLYGONS = "multipolygons"

    type_features = Option(Geometry, required=True, positional=True)

    def __call__(self, content):
        tmp_osm = tempfile.NamedTemporaryFile(mode="wb", delete=False)
        tmp_osm.write(content)
        tmp_osm.close()
        try:
            proc = subprocess.run(
                args=[
                    "ogr2ogr",
                    "-f",
                    "GeoJSON",
                    "/vsistdout/",
                    tmp_osm.name,
                    self.type_features.value,
                    "--config",
                    "OSM_USE_CUSTOM_INDEXING",
                    "NO",
                    "-lco",
                    "COLUMN_TYPES=all_tags",
                ],
                stdout=subprocess.PIPE,
                stderr=subprocess.PIPE,
                encoding="utf8",
            )
            value = proc.stdout
            if proc.returncode != 0:
                logger.info(value)
                logger.error(proc.stderr)
                raise RuntimeError(
                    "Command ogr2ogr failed with exit code {proc.returncode}"
                )
        except subprocess.CalledProcessError as e:
            raise RuntimeError("Command ogr2ogr failed") from e
        finally:
            os.unlink(tmp_osm.name)

        yield value


class Ogr2ogrGeojson2Geojson(Configurable):
    """
    Transform other_tags from ogr2ogr as standard properties

      dict record
    """

    def __call__(self, record):
        try:
            other_tags_str = record.pop("other_tags")
            if other_tags_str:
                other_tags = dict(
                    [a.split('"=>"') for a in other_tags_str.strip('"').split('","')]
                )
                record.update(other_tags)
        except ValueError as e:
            raise ValueError(f'Fails to parse "other_tags": {other_tags_str}') from e
        except KeyError:
            pass

        yield record
