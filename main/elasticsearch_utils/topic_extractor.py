from collections import defaultdict
from datetime import datetime
from itertools import combinations

FILTER_THRESHOLD = 0.5
MIN_COMMON_WORDS = 1
MAX_COMMON_WORDS = 6

MIN_ARTICLES_FOR_TOPIC = 1


def get_analyzed_data(raw_data):
    # filter out only articles with labels
    result = list(extract_valid_articles(raw_data))
    taxonomy_dict = defaultdict(lambda: defaultdict(list))

    for article in result:
        main_source = article['_source']
        for first_level_taxonomy in main_source['taxonomy']:
            if first_level_taxonomy['score'] < FILTER_THRESHOLD:
                continue
                
            # filter labels
            valid_labels = [x['text'] for x in main_source['labels'] if x.get('basedScore', 0) >= FILTER_THRESHOLD]
            added_labels = list(set(taxonomy_dict[first_level_taxonomy['text']]['label_texts'] + valid_labels))
            article['valid_labels'] = valid_labels
            taxonomy_dict[first_level_taxonomy['text']]['articles'].append(article)
            taxonomy_dict[first_level_taxonomy['text']]['labels'] = added_labels
    return remove_elements_by_key_length(taxonomy_dict, MIN_ARTICLES_FOR_TOPIC)


def extract_valid_articles(raw_data):
    for elem in raw_data['hits']:
        if elem and elem.get('_source', {}).get('labels'):
            yield elem


def get_level_labels(input_combinations, labels):
    # to generate next level topics only from topics with (count of words - 1)
    output_labels = []
    for combination in input_combinations:
        for label in labels:
            if label in combination:
                continue
                
            output_labels.append(tuple(sorted(combination + tuple([label]))))
    return list(set(output_labels))


def remove_elements_by_key_length(dictionary, length):
    return {key: value for key, value in dictionary.items() if len(value) >= length}


def get_trends_taxonomies(analyzed_taxonomies):
    topic_dict = defaultdict(lambda: defaultdict(list))
    for taxonomy, elements in analyzed_taxonomies.items():
        # generate first level combinations by itertools
        label_combinations = list(combinations(elements['labels'], MIN_COMMON_WORDS))
        for step in range(MIN_COMMON_WORDS, MAX_COMMON_WORDS + 1):
            for key in label_combinations:
                for article in elements['articles']:
                    if set(key).issubset(article['valid_labels']):
                        article_info = {
                            'article_id': article['id'],
                            'lead': article['lead'],
                            'title': article['title'],
                            'url': article['url'],
                        }
                        if key in topic_dict[taxonomy].keys():
                            topic_dict[taxonomy][key].append(article_info)
                        else:
                            topic_dict[taxonomy][key] = [article_info]

            filtered_topics = remove_elements_by_key_length(topic_dict[taxonomy], 2)
            if len(filtered_topics) > 0:
                topic_dict[taxonomy] = filtered_topics
            else:
                topic_dict.pop(taxonomy)

            label_combinations = get_level_labels([key for key in topic_dict[taxonomy].keys() if len(key) >= step],
                                                  elements['labels'])
    return remove_elements_by_key_length(topic_dict, 1)


def get_related_articles(article_info):
    for article in article_info:
        yield {
            'title': article['article_title'],
            'url': article['url'],
            'lead': article['article_lead'],
            'article_id': article['article_id'],
        }


def make_topic_instance(label_name, article_info, taxonomy, start_date):
    topic_name = '_'.join((taxonomy, '_'.join(label_name)))
    date_now = datetime.now().date().isoformat()
    if start_date == 'now/d-24h':
        start_date = date_now
    return {
        'id': '_'.join((start_date, topic_name)),
        'topic_name': topic_name,
        'topic_id': topic_name,
        'relatedArticles': list(get_related_articles(article_info)),
        'hour_time': start_date,
        'created_at': date_now,
        'refreshed_at': date_now,
    }
