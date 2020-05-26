import json

import boto3
from elasticsearch import (
    Elasticsearch,
    RequestsHttpConnection,
)
from requests_aws4auth import AWS4Auth


def get_es_instance(host, port):
    credentials = boto3.Session().get_credentials()
    awsauth = AWS4Auth(
        credentials.access_key,
        credentials.secret_key,
        'AWS-AZ',
        'es',
        session_token=credentials.token,
    )
    return Elasticsearch(
        hosts=[{'host': host, 'port': port}],
        http_auth=awsauth,
        use_ssl=True,
        timeout=25,
        verify_certs=True,
        max_retries=10,
        retry_on_timeout=True,
        connection_class=RequestsHttpConnection,
    )


def validate_time_format(time_info):
    try:
        if not isinstance(time_info, str):
            time_info = time_info.strftime('%Y-%m-%d')
        return time_info

    except ValueError:
        raise ValueError('Incorrect data format, should be YYYY-mm-dd')


def search_data_in_es(es_client, start_time, end_time, provider_id, index_name='some_index'):
    start_time = validate_time_format(start_time)
    end_time = validate_time_format(end_time)
    es_query = {
        "size": 10000,
        "query": {
            "bool": {
                "must": [
                    {
                        "nested": {
                            "path": "someExamplePath",
                            "query": {
                                "bool": {
                                    "must": [
                                        {
                                            "match": {
                                                "provider_id": provider_id
                                            }
                                        }
                                    ]
                                }
                            }
                        }
                    }
                ],
                "filter": {
                    "range": {
                        "hour_time": {
                            "gte": start_time,
                            "lte": end_time
                        }
                    }
                }
            }
        }
    }
    return es_client.search(
        index=index_name,
        scroll='1m',
        ignore_unavailable=True,
        body=json.dumps(es_query),
    )
