package com.example.kafka.serializer;

import com.example.kafka.model.Message;
import lombok.extern.slf4j.Slf4j;
import org.apache.avro.Schema;
import org.apache.avro.io.DatumReader;
import org.apache.avro.io.Decoder;
import org.apache.avro.io.DecoderFactory;
import org.apache.avro.generic.GenericDatumReader;

import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.time.Instant;
import java.util.zip.GZIPInputStream;

@Slf4j
public class AvroKafkaDeserializer implements org.apache.kafka.common.serialization.Deserializer<Message> {

    private final DatumReader<org.apache.avro.generic.GenericRecord> reader;
    private final Schema schema;

    public AvroKafkaDeserializer() {
        this.schema = loadSchema();
        this.reader = new GenericDatumReader<>(schema);
    }

    private Schema loadSchema() {
        try (java.io.InputStream is = getClass().getClassLoader().getResourceAsStream("avro/KafkaMessage.avsc")) {
            if (is == null) {
                throw new RuntimeException("Schema not found: avro/KafkaMessage.avsc");
            }
            return new Schema.Parser().parse(is);
        } catch (IOException e) {
            throw new RuntimeException("Failed to load Avro schema", e);
        }
    }

    @Override
    public void configure(java.util.Map<String, ?> configs, boolean isKey) {
    }

    @Override
    public Message deserialize(String topic, byte[] data) {
        if (data == null || data.length == 0) return null;

        try (ByteArrayInputStream bais = new ByteArrayInputStream(data);
             GZIPInputStream gzip = new GZIPInputStream(bais)) {

            Decoder decoder = DecoderFactory.get().directBinaryDecoder(gzip, null);
            org.apache.avro.generic.GenericRecord record = reader.read(null, decoder);

            Message message = Message.builder()
                    .id(String.valueOf(record.get("id")))
                    .topic(String.valueOf(record.get("topic")))
                    .key(record.get("key") != null ? record.get("key").toString() : null)
                    .value(String.valueOf(record.get("value")))
                    .partition((Integer) record.get("partition"))
                    .offset((Long) record.get("offset"))
                    .timestamp(Instant.ofEpochMilli((Long) record.get("timestamp")))
                    .build();

            log.debug("[AVRO-DESERIALIZE] topic={} id={}", topic, message.getId());
            return message;

        } catch (IOException e) {
            throw new RuntimeException("Failed to deserialize Avro message from topic " + topic, e);
        }
    }

    @Override
    public void close() {
    }
}