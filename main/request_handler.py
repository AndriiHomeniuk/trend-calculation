import logging
import os
from datetime import datetime, timedelta

from .elasticsearch_utils.es_request_utils import (
    search_data_in_es,
    get_es_instance,
)
from .main_calculation import calculate_trending_topics

logger = logging.getLogger('trend')
FORMAT = '%(levelname)s %(message)s'
logging.basicConfig(format=FORMAT)
es_client = get_es_instance(
    host=os.environ['ELASTICSEARCH_HOST'],
    port=int(os.environ['ELASTICSEARCH_PORT']),
)


def handle(event, _):
    logger.debug(event)
    if 'PING' in event:
        return {'PONG': event['PING']}

    end_time = event.get('end_time', datetime.now().date().isoformat())
    start_time = event.get('start_time', (datetime.now() - timedelta(days=1)).isoformat())
    return calculate_trending_topics(
        data=search_data_in_es(
            es_client=es_client,
            start_time=start_time,
            end_time=end_time,
            provider_id='example_id',
        ),
        start_time=start_time,
        end_time=end_time,
    )
