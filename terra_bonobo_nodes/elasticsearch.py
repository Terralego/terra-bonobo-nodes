import logging
from bonobo.config import Configurable, Option, Service
from bonobo.config.processors import ContextProcessor
from bonobo.constants import END, NOT_MODIFIED
from bonobo.util.objects import ValueHolder
from elasticsearch import client
from elasticsearch import helpers

logger = logging.getLogger(__name__)


class ESExtract(Configurable):
    index_name = Option(str, required=False, positional=True)
    body = Option(dict, positional=True, default={"query": {"match_all": {}}})

    es = Service('es')

    def __call__(self, es):
        page = es.search(
            index=self.index_name,
            body=self.body,
            scroll='2m',
            size=5000
        )

        sid = page['_scroll_id']
        scroll_size = page['hits']['total']

        while (scroll_size > 0):
            for hit in page['hits']['hits']:
                yield hit['_id'], hit['_source']

            page = es.scroll(scroll_id=sid, scroll='2m')
            sid = page['_scroll_id']
            scroll_size = len(page['hits']['hits'])


class LoadInES(Configurable):
    index = Option(str, required=True, positional=True)
    length = 1000

    es = Service('es')

    @ContextProcessor
    def buffer(self, context, *args, **kwargs):
        buffer = yield ValueHolder([])

        if len(buffer):
            context.input._writable_runlevel += 1
            context.input._runlevel += 1
            context.input.put((END, END))
            context.input.put(END)
            # context.input._runlevel += 2
            context.step()

    def __call__(self, buffer, identifier, properties, es, *args, **kwargs):
        is_final = identifier == END and properties == END

        if not is_final:
            buffer.append(self._get_formated_record(identifier, properties))

        if len(buffer) >= self.length or is_final:
            try:
                helpers.bulk(es, buffer.get(), stats_only=True)
            except helpers.BulkIndexError as e:
                logger.error(f'Indexing error: {e}')

            buffer.set([])

        return NOT_MODIFIED

    def _get_formated_record(self, identifier, properties):
        return {
            '_index': self.index,
            '_type': '_doc',
            '_id': identifier,
            '_source': properties,
        }


class ESGeometryField(Configurable):
    index = Option(str, required=True, positional=True)
    geom_field = Option(str, required=True, positional=True)
    total_fields = Option(int, default=10000, positional=True)

    es = Service('es')

    def __call__(self, es, *args, **kwargs):
        indice = client.IndicesClient(es)
        if not indice.exists(index=self.index):
            indice.create(index='features')
            indice.put_mapping(
                index='features',
                doc_type='_doc',
                body={
                    'properties': {
                        self.geom_field: {
                            'type': 'geo_shape',
                            'ignore_z_value': True,
                            'tree': 'quadtree',
                            }
                    },
                }
            )
            indice.put_settings(
                index='features',
                body={
                    "index.mapping.total_fields.limit": self.total_fields
                },
            )

        return NOT_MODIFIED


class ESOptimizeIndexing(Configurable):
    index = Option(str, required=True, positional=True)

    es = Service('es')

    def __call__(self, es):
        es = client.IndicesClient(es)
        cluster = client.ClusterClient(es)
        es.put_settings(
            index=self.index,
            body={
                "index.refresh_interval": -1,
            }
        )
        cluster.put_settings(
            body={
                "transient": {
                    "archived.index.store.throttle.type": None,
                },
            },
        )

        yield NOT_MODIFIED
