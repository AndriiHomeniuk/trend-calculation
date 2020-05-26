from collections import defaultdict


def group_topics_by_providers(topic_entities):
    grouped_by_providers = defaultdict(defaultdict)
    for item in topic_entities:
        for article in item['relatedArticles']:
            provider_name = article['provider']
            topic_name = item['topic_name']
            if topic_name not in grouped_by_providers[provider_name]:
                grouped_by_providers[provider_name][topic_name] = []
            grouped_by_providers[provider_name][topic_name].append(article)
    return grouped_by_providers


def calculate_trending_topics(data, start_time, end_time):
    grouped_by_providers = group_topics_by_providers(data.get('hits'))
    trending_topics = []
    for provider_name, topics in grouped_by_providers.items():
        for topic_name, articles in topics.items():
            trending_topics.append({
                'provider': provider_name,
                'topic': topic_name,
                'related_articles': [prepare_related_article(article) for article in articles],
                'trendiness_score': len(articles),
                'timeFrameStart': start_time,
                'timeFrameEnd': end_time,
            })
    trending_topics.sort(key=lambda item: item.get('trendiness_score', 0), reverse=True)
    return trending_topics


def prepare_related_article(article):
    return {
        'title': article['title'],
        'url': article['url'],
        'article_id': article['article_id'],
    }
