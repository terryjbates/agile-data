#!/usr/bin/env python

import imaplib
import getpass
from avro import schema, datafile, io
import re
import argparse


MAILBOX = 'bala'
OUTFILE = '/tmp/my_emails.avro'

#    headers = "Content-Transfer-Encoding Content-Type Date Delivered-To From In-Reply-To MIME-Version Message-ID Received-SPF Received References Return-Path Subject To X-Gmail-Received X-Mailer".split()

SCHEMA_STR = """{
    "type": "record",
    "name": "rawEmail",
    "fields" : [
      { "name": "Message_ID", "type": "string"},
      { "name":"Date", "type": ["string", "null"] },
      { "name":"From", "type": ["string", "null"] },
      { "name":"To", "type": ["string", "null"] },
      { "name":"Subject", "type": ["string", "null"] }
    ]
}"""

SCHEMA = schema.parse(SCHEMA_STR)
# Create a 'record' (datum) writer
rec_writer = io.DatumWriter(SCHEMA)

# # Create a 'data file' (avro file) writer
# df_writer = datafile.DataFileWriter(
#   open(OUTFILE_NAME, 'wb'),
#   rec_writer,
#   writers_schema = SCHEMA
# )

def obtain_df_writer(filename):
    return  datafile.DataFileWriter(
        open(filename, 'wb'),
        rec_writer,
        writers_schema = SCHEMA
        )



def read_messages(imap, mbox, df_writer):
    # obtain all email messages
    typ, data = imap.search(None, 'ALL')
    
    # create a pattern to extract headers
    #pattern = re.compile('^(.+)\: (.+)')
    pattern = re.compile('^(.+?)\: (.+)$')
    # get a list of headers ready
    # This may need to be dynamic for other us. Gmail may omit headers.
    #headers = "Content-Transfer-Encoding Content-Type Date Delivered-To From In-Reply-To MIME-Version Message-ID Received-SPF Received References Return-Path Subject To X-Gmail-Received X-Mailer".split()
    headers = "Message-ID From To Subject Date".split()


    for num in data[0].split():
        typ, data = imap.fetch(num, '(RFC822)')
        #print 'Message %s\n%s\n' % (num, data[0][1])

        # We obtain the actual raw message, since it is one
        # huge string, multi-line string, we make a list and split it on lines
        raw_message = data[0][1].splitlines()

        # create a dictionary to store header info per message
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
        df_writer.append({"Message_ID": header_dict['Message-ID'], "From": header_dict['From'],"Subject": header_dict['Subject'], "To": header_dict['To'], "Date": header_dict['Date']})

            
    # Close the write on completion    
    df_writer.close()
               
def init_imap(username, password, mbox):
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

    # obtain the status and the message count
    status, count = imap.select(mbox)

    # return the imap object and the message count
    return imap, count

def main():
    parser = argparse.ArgumentParser (
        description = 'script to scrap Gmail via IMAP')

    parser.add_argument(
        '--file', type=str,
        help='output file of downloaded emails to in .avro format',
        default=OUTFILE)

    parser.add_argument(
        '--mbox', type=str,
        help='mailbox we wish to scrape',
        default=MAILBOX)

    args = parser.parse_args()

    # Prompt for gmail user/pass
    username = raw_input("please enter your username: ")
    password = getpass.getpass("please enter your password: ")

    df_writer = obtain_df_writer(args.file)

    # We return the total count of messages and the imap object
    (imap, count) = init_imap(username, password, args.mbox)
    print "Count is " , count
    read_messages(imap, args.mbox, df_writer)
    imap.close()

if __name__ == '__main__':
    main()
    #pass
