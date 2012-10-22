#!/usr/bin/env python
from avro import schema, datafile, io
import pprint
OUTFILE_NAME = '/tmp/messages.avro'
SCHEMA_STR = """{
    "type": "record",
    "name": "Message",
    "fields" : [
      {"name": "message_id", "type": "int"},
      {"name": "topic", "type": "string"},
      {"name": "user_id", "type": "int"}
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

df_writer.append( {"message_id": 11, "topic": "Hello galaxy", "user_id": 1} )
df_writer.append( {"message_id": 12, "topic": "Jim is silly!", "user_id": 1} )
df_writer.append( {"message_id": 23, "topic": "I like apples.", "user_id": 2} )
df_writer.close()