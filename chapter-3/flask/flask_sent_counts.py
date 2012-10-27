#!/usr/bin/env python
from pymongo import Connection
import json
from flask import Flask

def print_dict(dict):
    for key, value in dict.iteritems():
        print key, value




app = Flask(__name__)
connection = Connection()
db = connection.agile_data

@app.route("/<input>")
def echo(input):
    return input

@app.route("/sent_counts/<ego1>/<ego2>")
def sent_counts(ego1, ego2):
    # ego1 = ego1.decode('utf8')
    # ego2 = ego2.decode('utf8')
    # print "We have from: %s  and to %s" % (ego1, ego2)
    # print "We have from type: %s  and to type %s" % (type(ego1), type(ego2))
    # print ego1
    # print ego2

    # /russell.jurney@gmail.com/bumper1700@hotmail.com

    #sent_count = db['sent_counts'].find_one({'ego1': ego1, 'ego2': ego2})
    #sent_count = db['sent_counts'].find_one({'from': ego1, 'to': ego2})
    terry = unicode('"terry bates" <terryjbates@gmail.com>')
    bala = unicode('"g. balasekaran" <g.balasekaran@mail.hgen.pitt.edu>')

    # print "bala"
    # print bala
    # print
    # print "terry"
    # print terry
    # find_bala_terry_result = db.sent_counts.find_one({'from': bala, 'to': terry})
    # print "our bala_terry sent result is", find_bala_terry_result
    # print "#" * 40

    # print "ego 1"
    # print ego1
    # print
    # print "ego 2"
    # print ego2


    find_search_result = db.sent_counts.find_one({'from': ego1, 'to': ego2})
    # print "Our normal search  result is", find_search_result
    # print "#" * 40

    # print_dict(find_search_result)

    #plain = {'from': sent_count['ego1'], 'to': sent_count['ego2'], 'total': sent_count['total']}
    plain = {'from': find_search_result['from'], 'to': find_search_result['to'], 'total': find_search_result['total']}
    # print "plain is ", plain
    # print "#" * 40
    return json.dumps(plain)

if __name__ == "__main__":
    #app.run()
    app.run(debug=True)
