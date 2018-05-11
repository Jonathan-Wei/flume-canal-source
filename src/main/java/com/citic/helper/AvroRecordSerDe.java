package com.citic.helper;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import org.apache.avro.Schema;
import org.apache.avro.generic.GenericDatumReader;
import org.apache.avro.generic.GenericDatumWriter;
import org.apache.avro.generic.GenericRecord;
import org.apache.avro.io.BinaryDecoder;
import org.apache.avro.io.BinaryEncoder;
import org.apache.avro.io.DatumReader;
import org.apache.avro.io.DatumWriter;
import org.apache.avro.io.DecoderFactory;
import org.apache.avro.io.EncoderFactory;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * The type Avro record ser de.
 */
public class AvroRecordSerDe {

    private static final Logger LOGGER = LoggerFactory.getLogger(AvroRecordSerDe.class);

    private static BinaryDecoder decoder;
    private static BinaryEncoder encoder;

    /**
     * Deserialize generic record.
     *
     * @param bytes the bytes
     * @param schema the schema
     * @return the generic record
     */
    private static GenericRecord deserialize(byte[] bytes, Schema schema) {
        DatumReader<GenericRecord> datumReader = new GenericDatumReader<>(schema);
        decoder = DecoderFactory.get().binaryDecoder(bytes, decoder);
        try {
            return datumReader.read(null, decoder);
        } catch (IOException e) {
            LOGGER.error(e.getMessage(), e);
            return null;
        }
    }

    /**
     * Serialize byte [ ].
     *
     * @param record the record
     * @param schema the schema
     * @return the byte [ ]
     */
    private static byte[] serialize(GenericRecord record, Schema schema) {
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
