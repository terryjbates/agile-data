#!/usr/bin/env python

import imaplib
import getpass
from avro import schema, datafile, io
import re
import argparse

# Import a modules to assist in extracting email headers
import email
import mail_parse

# Use this to try and prevent unicode/ascii errors
import sys
reload(sys)
sys.setdefaultencoding('utf8')

MAILBOX = 'bala'
OUTFILE = '/tmp/my_emails.avro'


SCHEMA_STR = """{
    "type": "record",
    "name": "rawEmail",
    "fields" : [
      { "name": "message_id", "type": ["string", "null"]},
      { "name":"date", "type": ["string", "null"] },
      { "name":"from", "type": ["string", "null"] },
      { "name":"to", "type": ["string", "null"] },
      { "name":"cc", "type": ["string", "null"] },
      { "name":"bcc", "type": ["string", "null"] },
      { "name":"reply_to", "type": ["string", "null"] },
      { "name":"in_reply_to", "type": ["string", "null"] },
      { "name":"subject", "type": ["string", "null"] },
      { "name":"body", "type": ["string", "null"] }
    ]
}"""

# Creat our schma object
SCHEMA = schema.parse(SCHEMA_STR)

# Create a 'record' (datum) writer
rec_writer = io.DatumWriter(SCHEMA)

def get_msg_body(msg):
    '''Function to extract the text body of a message.'''
  for part in msg.walk():
    if part.get_content_type() == "text/plain":
        #yield part.get_payload(decode=1)    
        return part.get_payload(decode=1)    
    else:
        continue


def process_email_header(email_header_field):
    '''This function returns only the email address, when given tuple of "From" or "To" email header field'''
    email_header_field =('', '') if not email_header_field else email_header_field[0]
    # If we have human name and email address, only grab the email address
    if len(email_header_field) == 2:
        email_header_field = email_header_field[1]
    # Otherwise, just grab the email address
    else:
        email_header_field = email_header_field[0]

    return email_header_field


def obtain_df_writer(filename):
    '''This returns a df writer object to send data to .avro file.'''
    return  datafile.DataFileWriter(
        open(filename, 'wb'),
        rec_writer,
        writers_schema = SCHEMA
        )


def read_messages(imap, mbox, df_writer):
    '''Connect to an IMAP server, given a particular inbox, write out the data to a df write object. '''
    # obtain all email messages (or as many as we will be given).
    typ, data = imap.search(None, 'ALL')


    for num in data[0].split():
        typ, data = imap.fetch(num, '(RFC822)')

        #print 'Message %s\n%s\n' % (num, data[0][1])
        # We obtain the actual raw message, since it is one
        # huge string, multi-line string.
        raw_message = data[0][1]
        msg = email.message_from_string(raw_message)
        
        # Obtain email subject
        subject = mail_parse.getmailheader(msg.get('Subject', ''))

        # 'From' and 'To' have the long form of email header
        # "Terry J Bates" <terryjbates@gmail.com>"
        # we only want the email address.

        from_ = mail_parse.getmailaddresses(msg, 'from')
        from_ = process_email_header(from_)

        tos = mail_parse.getmailaddresses(msg, 'to')
        tos = process_email_header(tos)

        print "From: %s To: %s" % (from_, tos)
        # obtain the date and message id header fields
        msg_date = mail_parse.getmailheader(msg.get('Date', ''))
        msg_cc = mail_parse.getmailheader(msg.get('CC', ''))
        msg_bcc = mail_parse.getmailheader(msg.get('Bcc', ''))
        msg_reply_to = mail_parse.getmailheader(msg.get('Reply-To', ''))
        msg_in_reply_to = mail_parse.getmailheader(msg.get('In-Reply-To', ''))

        try:
            msg_body = get_msg_body(msg)
        except:
            msg_body = ''

        # deal with empty message IDs
        if not msg_id:
            msg_id = "NULL"
        
        # deal with empty 'To' fields
        if not tos:
            tos = "<terryjbates@gmail.com>"

        # deal with unicode strings in subject lines
        try:
            subject = subject.decode('utf8')
        except UnicodeEncodeError, e:
            print "we have unicode encode error with %r" % subject
            print "Exception is %s", e
            # Beak out totally if we have no email subject
            break

        try:
            from_ = from_.decode('utf8')
            tos = tos.decode('utf8')
            #df_writer.append({"Message_ID": msg_id, "From": from_,"Subject": subject, "To": tos, "Date": msg_date})
            df_writer.append({"message_id": msg_id, "date": msg_date, "from": from_, "to": tos, "cc": msg_cc, "bcc": msg_bcc, "reply_to": msg_reply_to, "in_reply_to": msg_in_reply_to, "subject": subject, "body": msg_body})
        except UnicodeDecodeError, e:
            print "we have unicode decode error with %r" % subject
            print "Exception is %s", e
            continue
        except UnicodeEncodeError, e:
            print "we have unicode encod error with %r" % subject
            print "Exception is %s", e
            continue

            
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
