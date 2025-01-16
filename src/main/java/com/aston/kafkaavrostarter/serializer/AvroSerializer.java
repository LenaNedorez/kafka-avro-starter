package com.aston.kafkaavrostarter.serializer;

import com.aston.kafkaavrostarter.exception.AvroSerializationException;
import lombok.extern.slf4j.Slf4j;
import org.apache.avro.Schema;
import org.apache.avro.generic.GenericData;
import org.apache.avro.generic.GenericDatumWriter;
import org.apache.avro.generic.GenericRecord;
import org.apache.avro.io.DatumWriter;
import org.apache.avro.io.Encoder;
import org.apache.avro.io.EncoderFactory;
import org.apache.kafka.common.serialization.Serializer;

import java.io.ByteArrayOutputStream;
import java.util.Map;

@Slf4j
public class AvroSerializer<T extends org.apache.avro.specific.SpecificRecordBase> implements Serializer<T> {
    private final EncoderFactory encoderFactory = EncoderFactory.get();

    @Override
    public void configure(Map<String, ?> configs, boolean isKey) {
        Serializer.super.configure(configs, isKey);
    }

    @Override
    public byte[] serialize(String topic, T data) {
        try (ByteArrayOutputStream out = new ByteArrayOutputStream()){
            Schema schema = data.getSchema();
            GenericRecord genericRecord = new GenericData.Record(schema);
            schema.getFields().forEach(field -> genericRecord.put(field.name(), data.get(field.name())));

            DatumWriter<GenericRecord> writer = new GenericDatumWriter<>(schema);
            Encoder encoder = encoderFactory.binaryEncoder(out, null);
            writer.write(genericRecord, encoder);
            encoder.flush();
            return out.toByteArray();

        } catch (Exception e) {
            log.error("Error during serialization", e);
            throw new AvroSerializationException("Error during serialization", e);
        }
    }

    @Override
    public void close() {
        Serializer.super.close();
    }
}
