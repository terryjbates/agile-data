#!/usr/bin/env python
import esclient 



def print_dict(dict):
    for key, value in dict.iteritems():
        print key, value
        print "#######"
        print


def main():
    es_object = esclient.ESClient("http://localhost:9200/")

    query_string_args = {
        'q':'terryjbates@gmail.com AND amazon'

    }
    result = es_object.search(query_string_args=query_string_args, indexes=['sent_counts'])
    #print_dict(result)
    print result['hits']['total']


if __name__ == '__main__':
    main()
