import elasticsearch
from terra_bonobo_nodes import elasticsearch as elasticsearch_terra
import unittest
from bonobo.util.testing import BufferingNodeExecutionContext
from unittest import mock
from bonobo.constants import NOT_MODIFIED


class Test_ES_Extract(unittest.TestCase):
    def test_es_extract(self):
        with mock.patch(
            'elasticsearch.Elasticsearch.search'
        ) as mock_es:
            es = elasticsearch.Elasticsearch()
            es_found = {
                "_scroll_id": "DXIF",
                "hits": {
                    "total": {
                        "value": 15,
                    },
                    "max_score": 1.0,
                    "hits": [
                        {
                            "_id": "1",
                            "_source": ...,
                        },
                        {
                            "_id": "2",
                            "_source": "obj.*",
                        }
                    ]
                }
            }
            mock_es.return_value = es_found
            esextract = elasticsearch_terra.ESExtract()
            with mock.patch(
                'elasticsearch.Elasticsearch.scroll'
            ) as mock_scroll:
                mock_scroll.return_value = {
                    '_scroll_id': '',
                    'hits': {
                        'hits': []
                    }
                }
                result = [row for row in esextract(es)]
                compteur = 0
                for row in es_found.get("hits").get("hits"):
                    self.assertEqual(row.get("_id"), result[compteur][0])
                    self.assertEqual(row.get("_source"), result[compteur][1])
                    self.assertEqual(2, len(row))
                    compteur += 1

    def test_loadlines_valid(self):
        index = "index"
        properties = {'layer': 'a'}
        id_test = "1"
        properties_2 = {'layer': 'b'}
        id_test2 = "2"

        with mock.patch('elasticsearch.helpers.bulk') as mock_es:
            es = elasticsearch.Elasticsearch()
            with BufferingNodeExecutionContext(
                    elasticsearch_terra.LoadInES(index=index),
                    services={'es': es}) as context:

                context.write_sync((id_test, properties),
                                   (id_test2, properties_2))

        self.assertTrue(mock_es.called)

    def test_loadlines_exception(self):
        index = "index"
        properties = {'layer': 'g'}
        id_test = "1"

        properties_2 = {'layer': 'e'}
        id_test2 = "2"

        with mock.patch('elasticsearch.helpers.bulk', ) as mock_bulk:
            mock_bulk.side_effect = elasticsearch.helpers.BulkIndexError('Simulated error')
            es = elasticsearch.Elasticsearch()
            with self.assertLogs(elasticsearch_terra.logger):
                with BufferingNodeExecutionContext(
                        elasticsearch_terra.LoadInES(index=index),
                        services={'es': es}) as context:
                    context.write_sync((id_test, properties), (id_test2, properties_2))

    def test_esgeometryfield(self):
        index = "index"
        geom_field = "geom_field"
        es = elasticsearch.Elasticsearch()
        esgeometryfield = elasticsearch_terra.ESGeometryField(index=index,
                                                              geom_field=geom_field)
        with mock.patch(
            'elasticsearch.client.IndicesClient'
        ) as mock_indiceclient:
            mock_indiceclient.return_value.exists.return_value = False
            result = esgeometryfield(es)

        self.assertTrue(mock_indiceclient.return_value.put_mapping.called)
        self.assertTrue(mock_indiceclient.return_value.put_settings.called)
        self.assertEqual(result, NOT_MODIFIED)

    def test_esoptimizeindexing(self):
        index = "index"
        es = elasticsearch.Elasticsearch()
        esoptimizeindexing = elasticsearch_terra.ESOptimizeIndexing(
            index=index)
        with mock.patch(
                'elasticsearch.client.IndicesClient') as mock_indiceclient:
            with mock.patch(
                    'elasticsearch.client.ClusterClient') as mock_clusterclien:
                result = esoptimizeindexing(es)

        self.assertTrue(mock_clusterclien.return_value.put_settings.called)
        self.assertTrue(mock_indiceclient.return_value.put_settings.called)
        self.assertEqual(result, NOT_MODIFIED)


if __name__ == '__main__':
    unittest.main()
