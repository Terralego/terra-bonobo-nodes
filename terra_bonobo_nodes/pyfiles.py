import asyncio
import logging

from bonobo.config import Configurable, Option, Service

logger = logging.getLogger(__name__)


class PyfilesExtract(Configurable):
    """
    Extract files from using pyfile module

    Options:
      `namespace` Namespace in storage
      `filename` Filename of the file
      `version` version of the file

    Services:
      `pyfiles_storage` Pyfile object
      `http` request.Session object

    Return:
      bytes file content
    """

    namespace = Option(str, required=True, positional=True)
    filename = Option(str, required=True, positional=True)
    version = Option(str, required=False, positional=True, default="latest")

    pyfile_storage = Service("pyfile_storage")
    http = Service("http")

    def __call__(self, pyfile_storage, http):
        event_loop = asyncio.new_event_loop()
        result = event_loop.run_until_complete(
            pyfile_storage.search(
                namespace=self.namespace, filename=self.filename, version=self.version
            )
        )

        if result is None or "url" not in result:
            raise RuntimeError(
                f"Fails extract from pyfiles {self.namespace} {self.filename} {self.version}"
            )

        url = result["url"]
        response = http.get(url)
        if not response.ok:
            logger.error(response.text)
            raise RuntimeError(f"Request fails: {url}")

        return response.content
