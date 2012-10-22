#!/usr/bin/env python

from avro import schema, datafile, io
import pprint
# Test reading avros
rec_reader = io.DatumReader()
#INFILE_NAME = '/tmp/messages.avro'
#INFILE_NAME = '/tmp/test-schema-messages.avro'
INFILE_NAME = '/tmp/myemails.avro'

# Open a 'data file' (avro file) reader
df_reader = datafile.DataFileReader(
  open(INFILE_NAME),
  rec_reader
)

# Read all records stored inside
pp = pprint.PrettyPrinter()
for record in df_reader:
  pp.pprint(record)