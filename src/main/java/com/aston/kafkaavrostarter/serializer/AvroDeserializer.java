package com.aston.kafkaavrostarter.serializer;

import com.aston.kafkaavrostarter.exception.AvroSerializationException;
import lombok.extern.slf4j.Slf4j;
import org.apache.avro.Schema;
import org.apache.avro.generic.GenericDatumReader;
import org.apache.avro.generic.GenericRecord;
import org.apache.avro.io.DatumReader;
import org.apache.avro.io.Decoder;
import org.apache.avro.io.DecoderFactory;
import org.apache.kafka.common.serialization.Deserializer;

import java.io.ByteArrayInputStream;
import java.lang.reflect.InvocationTargetException;
import java.util.Map;

@Slf4j
public class AvroDeserializer<T extends org.apache.avro.specific.SpecificRecordBase> implements Deserializer<T> {
    private final DecoderFactory decoderFactory = DecoderFactory.get();
    private Class<T> avroClass;

    public AvroDeserializer(Class<T> avroClass) {
        this.avroClass = avroClass;
    }

    public AvroDeserializer(){}

    @Override
    public void configure(Map<String, ?> configs, boolean isKey) {
        Deserializer.super.configure(configs, isKey);
        if(avroClass == null && configs.containsKey("avro.class")) {
            String classname = String.valueOf(configs.get("avro.class"));
            try {
                this.avroClass = (Class<T>) Class.forName(classname);
            } catch (ClassNotFoundException e) {
                log.error("Error finding class in configuration", e);
                throw new AvroSerializationException("Error finding class in configuration", e);
            }
        }
    }

    @Override
    public T deserialize(String topic, byte[] data) {
        try {
            if (data == null) {
                return null;
            }

            Schema schema = avroClass.getDeclaredConstructor().newInstance().getSchema();

            DatumReader<GenericRecord> reader = new GenericDatumReader<>(schema);
            Decoder decoder = decoderFactory.binaryDecoder(new ByteArrayInputStream(data), null);
            GenericRecord genericRecord = reader.read(null, decoder);

            T result = avroClass.getDeclaredConstructor().newInstance();

            schema.getFields().forEach(field -> result.put(field.name(), genericRecord.get(field.name())));

            return result;

        } catch (InstantiationException | IllegalAccessException | NoSuchMethodException | InvocationTargetException e) {
            log.error("Error during deserialization - reflection error", e);
            throw new AvroSerializationException("Error during deserialization - reflection error", e);
        }
        catch (Exception e) {
            log.error("Error during deserialization", e);
            throw new AvroSerializationException("Error during deserialization", e);
        }
    }

    @Override
    public void close() {
        Deserializer.super.close();
    }
}
