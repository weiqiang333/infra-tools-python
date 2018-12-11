#!/usr/bin/env python

import sys
import time

from elasticsearch import Elasticsearch


log_name = 'production-access-log' if len(sys.argv) < 2 else sys.argv[1]
es = Elasticsearch(
    ['es0', 'es1', 'es2'],
    sniff_on_start=True,
    sniff_on_connection_fail=True,
    sniffer_timeout=1,
    http_compress=True
)
index = '{}-{}'.format(
    log_name, time.strftime('%Y.%m.%d', time.localtime(time.time()))
)
percentile_index = '{}-percentile-{}'.format(
    log_name, time.strftime('%Y.%m.%d', time.localtime(time.time()))
)

time_range = {
    '@timestamp': {
        'gt': 'now-5m',
        'lt': 'now'
    }
}


def get_services():
    body = {
        '_source': 'service',
        'size': 50,
        'query': {
            'bool': {
                'must': {
                    'range': time_range
                }
            }
        },
        'collapse': {
            'field': 'service.keyword'
        }
    }
    try:
        r = es.search(index=index, body=body)
        service_list = [hit.get('_source').get('service') for hit in r.get('hits').get('hits')]
    except Exception as e:
        print(time_range, e)
    return service_list


def get_dates(service):
    body = {
        'size': 0,
        'query': {
            'bool': {
                'must': [{
                    'range': time_range
                }, {
                    'match': {
                        'service': service
                    }
                }]
            }
        },
        'aggs': {
            'sales_over_time': {
                'date_histogram': {
                    'field': '@timestamp',
                    'interval': 'minute'
                }
            }
        }
    }
    r = es.search(index=index, body=body)
    bucket = r.get('aggregations').get('sales_over_time').get('buckets')
    date_bucket = [
        [k.get('key_as_string'), bucket[i+1].get('key_as_string'), k.get('doc_count')]
        for i, k in enumerate(bucket) if i + 1 < len(bucket)
    ]
    return date_bucket


def sort(date_bucket, service):
    percentiles = {}
    for key in date_bucket:
        start, end, count = key[0], key[1], key[2]
        sort_body = {
            'size': count * 0.99,
            '_source': ['@timestamp', 'response_time', 'request_path'],
            'query': {
                'bool': {
                    'must': [{
                        'range': {
                            '@timestamp': {
                                'gte': start,
                                'lt': end
                            }
                        }
                    }, {
                        'match': {
                            'service': service
                        }
                    }]
                }
            },
            'sort': {'response_time': {'order': 'asc'}}
        }
        r = es.search(index=index, body=sort_body)
        cache_hits = r.get('hits').get('hits')
        try:
            percentiles[start] = {
                '99th': cache_hits[int(count * 0.99 - 1)].get('_source', 0),
                '95th': cache_hits[int(count * 0.95 - 1)].get('_source', 0),
                '80th': cache_hits[int(count * 0.80 - 1)].get('_source', 0),
                '50th': cache_hits[int(count * 0.50 - 1)].get('_source', 0)
            }
        except Exception as e:
            print('{}\t{}\n{}'.format(service, start, e))
    return percentiles


def insert():
    for service in get_services():
        for key, value in sort(get_dates(service), service).items():
            body = {
                'size': 0,
                'query': {
                    'bool': {
                        'must': [
                            {'match': {'service': service}},
                            {'match': {'@timestamp': key}}
                        ]
                    }
                }
            }
            r = es.search(index=percentile_index, body=body, ignore=404)
            count = 0 if 'error' in r else r.get('hits').get('total')

            body = {
                '@timestamp': key,
                'service': service,
                '99th': value.get('99th'),
                '95th': value.get('95th'),
                '80th': value.get('80th'),
                '50th': value.get('50th'),
                'msg': [value]
            }
            if count == 0:
                try:
                    print(body)
                    r = es.index(index=percentile_index, doc_type='log', body=body)
                    print(r)
                except Exception as e:
                    print('{}\t{}\t{}\n{}'.format(service, key, value, e))


def main():
    insert()


if __name__ == '__main__':
    main()
