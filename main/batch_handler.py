import logging
import os

from .elasticsearch_utils.topic_extractor import (
    get_analyzed_data,
    get_trends_taxonomies,
    make_topic_instance,
)
from .update_wrapper import UpdateRecordWrapper

logger = logging.getLogger('appLoger')
logging.basicConfig(format='%(levelname)s %(message)s')
LOG_LEVEL = os.getenv('LOG_LEVEL', 'INFO')
logger.setLevel(LOG_LEVEL)
update_wrapper = UpdateRecordWrapper()
default_sources = [
    'contentId',
    'metadata.uri',
    'content.lead',
    'content.title',
    'contentProvider',
]


def handle(event, _):
    logger.debug(event)
    if 'PING' in event:
        return {'PONG': event['PING']}

    # load data from ES
    start_date = event.get('start_date', 'now/d-24h')  # yesterday's info
    end_date = event.get('end_date', 'now/d')
    sources = event.get('sources', default_sources)
    raw_data = update_wrapper.load_info(start_date, end_date, sources)

    # aggregate topics from raw_data
    formatted_data = get_analyzed_data(raw_data)
    trends = get_trends_taxonomies(formatted_data)

    # upload topics to ES
    if update_wrapper.check_index():
        for taxonomy, labels_info in trends.items():
            for label_name, article_info in labels_info.items():
                topic_instance = make_topic_instance(label_name, article_info, taxonomy, start_date)
                update_wrapper.index(
                    body=topic_instance,
                    content_id=topic_instance['id'],
                    index='topics',
                )
    return True
