package com.citic.helper;

import org.apache.avro.Schema;
import org.apache.avro.generic.GenericData;
import org.apache.avro.generic.GenericDatumReader;
import org.apache.avro.generic.GenericDatumWriter;
import org.apache.avro.generic.GenericRecord;
import org.apache.avro.io.*;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.ByteArrayOutputStream;
import java.io.IOException;

public class AvroRecordSerDe {
    private static final Logger LOGGER = LoggerFactory.getLogger(AvroRecordSerDe.class);

    private static BinaryDecoder decoder;
    private static BinaryEncoder encoder;

    public static GenericRecord deserialize(byte[] bytes, Schema schema) {
        DatumReader<GenericRecord>  datumReader = new GenericDatumReader<>(schema);
        decoder = DecoderFactory.get().binaryDecoder(bytes, decoder);
        try {
            return datumReader.read(null, decoder);
        } catch (IOException e) {
            LOGGER.error(e.getMessage(), e);
            return null;
        }
    }

    public static byte[] serialize(GenericRecord record, Schema schema) {
        DatumWriter<GenericRecord> datumWriter = new GenericDatumWriter<>(schema);
        ByteArrayOutputStream stream = new ByteArrayOutputStream();
        encoder = EncoderFactory.get().binaryEncoder(stream, encoder);
        try {
            datumWriter.write(record, encoder);
            encoder.flush();
            return stream.toByteArray();
        } catch (IOException e) {
            LOGGER.error(e.getMessage(), e);
            return new byte[0];
        }

    }
}
