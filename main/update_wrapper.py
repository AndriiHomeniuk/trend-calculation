import json
import os

from .elasticsearch_utils.es_request_utils import get_es_instance


class UpdateRecordWrapper:

    def __init__(self):
        self.es_client = get_es_instance(
            host=os.environ['ELASTICSEARCH_HOST'],
            port=int(os.environ['ELASTICSEARCH_PORT']),
        )

    def check_index(self, index_name='topics'):
        settings = {
            "settings": {
                "number_of_shards": 1,
                "number_of_replicas": 0
            },
            "mappings": {
                "_doc": {
                    "properties": {
                        "id": {"type": "keyword"},
                        "topic_name": {"type": "keyword"},
                        "topic_id": {"type": "keyword"},
                        "relatedArticles": {
                            "type": "nested",
                            "properties": {
                                "title": {"type": "text"},
                                "url": {"type": "text"},
                                "lead": {"type": "text"},
                                "article_id": {"type": "keyword"},
                            }
                        },
                        "hour_time": {
                            "type": "date",
                            "format": "yyyy-MM-dd"
                        },
                        "created_at": {
                            "type": "date",
                            "format": "yyyy-MM-dd"
                        },
                        "refreshed_at": {
                            "type": "date",
                            "format": "yyyy-MM-dd"
                        }
                    }
                }
            }
        }
        try:
            if not self.es_client.indices.exists(index_name):
                self.es_client.indices.create(
                    index=index_name,
                    body=settings,
                )
                print('Created Index')

        except Exception as ex:
            print(str(ex))

        finally:
            return self.es_client.indices.exists(index_name)

    def load_info(self, start_date, end_date, sources):
        es_query = {
            "_source": sources,
            "size": 10000,
            "query": {
                "bool": {
                    "must": [
                        {
                            "match": {
                                "someType": "someRequiredType"
                            }
                        },
                        {
                            "exists": {
                                "field": "someRequiredField"
                            }
                        }
                    ],
                    "filter": {
                        "range": {
                            "metadata.createdAt": {
                                "gte": str(start_date),
                                "lte": str(end_date)
                            }
                        }
                    }
                }
            }
        }
        return self.es_client.search(
            index='searched_index',
            scroll='10m',
            ignore_unavailable=True,
            body=json.dumps(es_query),
        )

    def index(self, body, content_id, index):
        return self.es_client.index(
            index=index,
            id=content_id,
            body=json.dumps(body),
        )
