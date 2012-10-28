#!/usr/bin/env python
'''
Script to read in values of agile_data mongoDB database, remove the extraneous text, which may contain first names, last names, and company names, and only grab the email address. Doing this will make the subsequent access via web interface less problematic due to the added text.
'''

# import for mail truncation
import re
import email
from email.Utils import parseaddr
from email.Header import decode_header

import pymongo
from pymongo import Connection


# Stolen from http://blog.magiksys.net/parsing-email-using-python-header
    
atom_rfc2822=r"[a-zA-Z0-9_!#\$\%&'*+/=?\^`{}~|\-]+"
atom_posfix_restricted=r"[a-zA-Z0-9_#\$&'*+/=?\^`{}~|\-]+" # without '!' and '%'
atom=atom_rfc2822
dot_atom=atom  +  r"(?:\."  +  atom  +  ")*"
quoted=r'"(?:\\[^\r\n]|[^\\"])*"'
local="(?:"  +  dot_atom  +  "|"  +  quoted  +  ")"
domain_lit=r"\[(?:\\\S|[\x21-\x5a\x5e-\x7e])*\]"
domain="(?:"  +  dot_atom  +  "|"  +  domain_lit  +  ")"
addr_spec=local  +  "\@"  +  domain

email_address_re=re.compile(addr_spec)


def update_db_record(to_addr, from_addr, object_id, db_collection):
    print "to:%s from:%s _id:%s" % (to_addr, from_addr, object_id)
    # We attempt to update the given object with our new values,
    # we use pymongo.objectid (though may be unnecessary),
    # set the 'to' ond 'from' address to our modded unicode values,
    # and do this safely
    try:
        db_collection.update({'_id': object_id}, {"$set" : {'to':to_addr, 'from':from_addr} }, safe=True)
    except :
        print "There was an operation failure with record %s" %(object_id)


def main ():
    # Open up our connection to mongod
    connection = Connection()

    # Connect to the database we are using
    db = connection.agile_data

    # Connect to the collection. In this case 'sent_counts'
    sent_counts = db.sent_counts
    # We will test with one record
    # test_record = sent_counts.find_one()

    # print "to %s" % (test_record['to'])
    # print "from %s" % (test_record['from'])

    # # We search for the email address, versus finding it.
    # # This strips out everything that does not match.
    # print email_address_re.search(test_record['to']).group(0)
    # print email_address_re.search(test_record['from']).group(0)
    # print dir(test_record)
    
    # Test to iterate through all records
    print "Our record count is %s", sent_counts.count()


    for sent_count in sent_counts.find():
        #print sent_count
        try:
            new_to_addr = unicode(email_address_re.search(sent_count['to']).group(0))
            new_from_addr = unicode(email_address_re.search(sent_count['from']).group(0))
            sent_count_id = sent_count['_id']
            update_db_record(new_to_addr, new_from_addr, sent_count_id, sent_counts)
        except AttributeError, e:
            print "Record %s has problem %s" % (sent_count['_id'], e)
     
        print


if __name__ =='__main__':
    main()
