from pyfiles import storages
from terra_bonobo_nodes import pyfiles as pyfiles_terra
import unittest
import requests
from unittest import mock
from bonobo.config import Service
import asyncio


class Test_TestPyfiles_PyfilesExtract(unittest.TestCase):
    async def a_coroutine(self):
        return {
            'url': 'whatever'
        }

    def test_pyfilesextract_valid(self):
        with mock.patch.object(
            storages.core, 'Storage'
        ) as mock_storage:
            mock_storage.return_value.search.return_value = self.a_coroutine()

            pyfile_storage = storages.core.Storage()

            request = requests.Session()
            with mock.patch.object(request, 'get',
                                   return_value=mock.Mock(ok=True)) as mock_get:
                content_wanted = "content"
                mock_get.return_value.content = content_wanted

                namespace = "namespace"
                filename = "filename"

                pyfilesextract = pyfiles_terra.PyfilesExtract(
                    namespace=namespace, filename=filename)


                result = pyfilesextract(pyfile_storage, request)
                self.assertEqual(result, content_wanted)

    def test_pyfilesextract_not_valid_response(self):
        with mock.patch.object(
            storages.core, 'Storage'
        ) as mock_storage:
            mock_storage.return_value.search.return_value = "whatever"
            with mock.patch.object(
                    asyncio, 'new_event_loop') as mock_event_loop:

                mock_event_loop.return_value.run_until_complete.return_value = None
                namespace = "namespace"
                filename = "filename"
                pyfilesextract = pyfiles_terra.PyfilesExtract(namespace=namespace,
                                                              filename=filename)
                http = Service('http')
                pyfile_storage = storages.core.Storage()

                with self.assertRaises(RuntimeError):
                    next(pyfilesextract(pyfile_storage, http))

    def test_pyfilesextract_not_valid_url(self):
        with mock.patch.object(
            storages.core, 'Storage'
        ) as mock_storage:
            mock_storage.return_value.search.return_value = "whatever"
            with mock.patch.object(
                asyncio, 'new_event_loop',
                    return_value=mock.Mock(ok=True)) as mock_event_loop: 
                mock_event_loop.return_value.run_until_complete.return_value = {
                        'url': 'oooo'}
                namespace = "namespace"
                filename = "filename"
                pyfilesextract = pyfiles_terra.PyfilesExtract(namespace=namespace,
                                                              filename=filename)
                pyfile_storage = storages.core.Storage()
                request = requests.Session()
                with mock.patch.object(request, 'get',
                                       return_value=mock.Mock(ok=False)):
                    with self.assertRaises(RuntimeError), self.assertLogs():
                        next(pyfilesextract(pyfile_storage, request))


if __name__ == '__main__':
    unittest.main()
