#!/usr/bin/env python

import imaplib
import getpass
from avro import schema, datafile, io
import re

MAILBOX = 'bala'
OUTFILE_NAME = '/tmp/myemails.avro'

#    headers = "Content-Transfer-Encoding Content-Type Date Delivered-To From In-Reply-To MIME-Version Message-ID Received-SPF Received References Return-Path Subject To X-Gmail-Received X-Mailer".split()

SCHEMA_STR = """{
    "type": "record",
    "name": "rawEmail",
    "fields" : [
      { "name": "thread_id", "type": "int"},
      { "name":"Date", "type": ["string", "null"] },
      { "name":"From", "type": ["string", "null"] },
      { "name":"To", "type": ["string", "null"] },
      { "name":"Subject", "type": ["string", "null"] }
    ]
}"""
SCHEMA = schema.parse(SCHEMA_STR)
# Create a 'record' (datum) writer
rec_writer = io.DatumWriter(SCHEMA)

# Create a 'data file' (avro file) writer
df_writer = datafile.DataFileWriter(
  open(OUTFILE_NAME, 'wb'),
  rec_writer,
  writers_schema = SCHEMA
)

# num = 5
# #df_writer.append( {"thread_id": "11", "raw_email": "Hello galaxy"})
# df_writer.append({"thread_id": 1, "X-Gmail-Received": "cc09b4acea40a207739a8e29311ad12a9c6ae154"})
# df_writer.append( {"thread_id": num, "X-Gmail-Received": "foobar"}) 
# df_writer.close()


def read_messages(imap, mbox):
    # obtain all email messages
    typ, data = imap.search(None, 'ALL')
    
    # create a pattern to extract headers
    #pattern = re.compile('^(.+)\: (.+)')
    pattern = re.compile('^(.+?)\: (.+)$')
    # get a list of headers ready
    # This may need to be dynamic for other us. Gmail may omit headers.
    #headers = "Content-Transfer-Encoding Content-Type Date Delivered-To From In-Reply-To MIME-Version Message-ID Received-SPF Received References Return-Path Subject To X-Gmail-Received X-Mailer".split()
    headers = "From To Subject Date".split()

    # start giving out thread ids at 1
    thread_id = 1
    for num in data[0].split():
        typ, data = imap.fetch(num, '(RFC822)')
        #print 'Message %s\n%s\n' % (num, data[0][1])

        # We obtain the actual raw message, since it is one
        # huge string, multi-line string, we make a list and split it on lines
        raw_message = data[0][1].splitlines()

        # creat a dictionary to store header info per message
        header_dict = {}
        
        for message_line in raw_message:
            message_pattern_match = re.match(pattern, message_line)
            
            # So, if we have an actual match object
            # and the first parenthetical match is in our 'headers list
            # and the value has not been 'seen' yet within our seen_dict
            if message_pattern_match and message_pattern_match.group(1) in headers and not header_dict.has_key(message_pattern_match.group(1)):
                # Added the 'seen' header to our dictionary
                # we do this to avoid multiple duplicated headers
                header_dict[message_pattern_match.group(1)] = message_pattern_match.group(2)
            else:
                pass
                #print "unmatched message line:\n%s\n\n" % message_line
        print header_dict    
        df_writer.append({"thread_id": thread_id, "From": header_dict['From'],"Subject": header_dict['Subject'], "To": header_dict['To'], "Date": header_dict['Date']})
        thread_id = thread_id + 1    
            
    # Close the write on completion    
    df_writer.close()
               
def init_imap(username, password, folder):
    # minimalistic code at bottom of http://docs.python.org/library/imaplib.html
    # import getpass, imaplib
    # 
    # M = imaplib.IMAP4()
    # M.login(getpass.getuser(), getpass.getpass())
    # M.select()
    # typ, data = M.search(None, 'ALL')
    # for num in data[0].split():
    #     typ, data = M.fetch(num, '(RFC822)')
    #     print 'Message %s\n%s\n' % (num, data[0][1])
    # M.close()
    # M.logout()    

    imap = imaplib.IMAP4_SSL('imap.gmail.com', 993)
    imap.login(username, password)
    # Use  imap.list() for a list of mailboxes
    # use imap.select(<name>) to choose one to work with
    # In our example, we use "bala"

    # 
    status, count = imap.select(folder)
    return imap, count

def main():
    username = raw_input("please enter your username: ")
    password = getpass.getpass("please enter your password: ")
    (imap, count) = init_imap(username,password,MAILBOX)
    print "Count is " , count
    read_messages(imap, MAILBOX)
    imap.close()

if __name__ == '__main__':
    main()
    #pass