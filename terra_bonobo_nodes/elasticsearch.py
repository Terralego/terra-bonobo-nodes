import logging

from bonobo.config import Configurable, Option, Service
from bonobo.config.processors import ContextProcessor
from bonobo.constants import END, NOT_MODIFIED
from bonobo.util.objects import ValueHolder
from elasticsearch import client, helpers

logger = logging.getLogger(__name__)


class ESExtract(Configurable):
    """
    Extract records from elasticsearch endpoint

    Services:
      `es` ElasticSearch-dsl object

    Options:
      `index_name` Index name where to make the query
      `body` ElasticSearch query, default: match_all

    Return:
      identifier, record
    """

    index_name = Option(str, required=False, positional=True)
    body = Option(dict, positional=True, default={"query": {"match_all": {}}})

    es = Service("es")

    def __call__(self, es):
        page = es.search(index=self.index_name, body=self.body, scroll="2m", size=5000)

        sid = page["_scroll_id"]
        scroll_size = page["hits"]["total"]["value"]

        while scroll_size > 0:
            for hit in page["hits"]["hits"]:
                yield hit["_id"], hit["_source"]

            page = es.scroll(scroll_id=sid, scroll="2m")
            sid = page["_scroll_id"]
            scroll_size = len(page["hits"]["hits"])


class LoadInES(Configurable):
    """
    Load record in an ElasticSearch index

    Options:
      `index` index name where to push the records

    Services:
      `es` ElasticSearch-dsl object

    Return:
      identifier, record
    """

    index = Option(str, required=True, positional=True)
    length = 1000

    es = Service("es")

    @ContextProcessor
    def buffer(self, context, *args, es, **kwargs):
        buffer = yield ValueHolder([])

        if len(buffer):
            # Final call if there is content in buffer
            self.__call__(buffer, END, END, es)

    def __call__(self, buffer, identifier, properties, es, *args, **kwargs):
        is_final = identifier == END and properties == END

        if not is_final:
            buffer.append(self._get_formated_record(identifier, properties))

        if len(buffer) >= self.length or is_final:
            try:
                helpers.bulk(es, buffer.get(), stats_only=True)
            except helpers.BulkIndexError as e:
                logger.error(f"Indexing error: {e}")

            buffer.set([])

        return NOT_MODIFIED

    def _get_formated_record(self, identifier, properties):
        return {
            "_index": self.index,
            "_id": identifier,
            "_source": {
                "_feature_id": identifier,
                **properties,
            },
        }


class ESGeometryField(Configurable):
    """
    Setup the geometry field in ElasticSearch index

    Options:
      `index` index name where to set the geom field
      `geom_field` the geom field name to set
      `total_fields` set the count of total fields, this is usefull in
        case of a big ES index

    Services:
      `es` ElasticSearch-dsl object

    Return:
      NOT_MODIFIED
    """

    index = Option(str, required=True, positional=True)
    geom_field = Option(str, required=True, positional=True)
    total_fields = Option(int, default=10000, positional=True)

    es = Service("es")

    def __call__(self, es, *args, **kwargs):
        indice = client.IndicesClient(es)
        if not indice.exists(index=self.index):
            indice.create(index=self.index)
            indice.put_mapping(
                index=self.index,
                body={
                    "properties": {
                        self.geom_field: {
                            "type": "geo_shape",
                            "ignore_z_value": True,
                        },
                        "_feature_id": {
                            "type": "keyword",
                        },
                    },
                },
            )
            indice.put_settings(
                index=self.index,
                body={"index.mapping.total_fields.limit": self.total_fields},
            )

        return NOT_MODIFIED


class ESOptimizeIndexing(Configurable):
    """
    Pre-indexing action to speedup indexing

    Options:
      `index` index name where to apply optimizations

    Services:
      `es` ElasticSearch-dsl object

    Return:
      NOT_MODIFIED
    """

    index = Option(str, required=True, positional=True)

    es = Service("es")

    def __call__(self, es):
        es = client.IndicesClient(es)
        cluster = client.ClusterClient(es)
        es.put_settings(
            index=self.index,
            body={
                "index.refresh_interval": -1,
            },
        )
        cluster.put_settings(
            body={
                "transient": {
                    "archived.index.store.throttle.type": None,
                },
            },
        )

        return NOT_MODIFIED
