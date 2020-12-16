import logging
import os
import subprocess
import tempfile

from bonobo.config import Configurable

logger = logging.getLogger(__name__)


class ZipShapefileToGeojson(Configurable):
    """
    Transforms zipped shapefile to geojson format

    Return:
      str geojson data
    """

    def __call__(self, content):
        tmp_zip = tempfile.NamedTemporaryFile(mode="wb", suffix=".zip", delete=False)
        tmp_zip.write(content)
        tmp_zip.close()
        try:
            proc = subprocess.run(
                args=[
                    "ogr2ogr",
                    "-t_srs",
                    "EPSG:4326",
                    "-f",
                    "GeoJSON",
                    "/vsistdout/",
                    f"/vsizip/{tmp_zip.name}",
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
            os.unlink(tmp_zip.name)

        yield value
