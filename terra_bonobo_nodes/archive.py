import io
from zipfile import ZipFile
from bonobo.config import Configurable, Option


class ZipReader(Configurable):
    content_paths = Option(list, required=False, positional=True, default=[])

    def __call__(self, content):
        with ZipFile(io.BytesIO(content)) as zipfile:
            for name in self.content_paths or zipfile.namelist():
                with zipfile.open(name) as content:
                    yield name, content.read()
