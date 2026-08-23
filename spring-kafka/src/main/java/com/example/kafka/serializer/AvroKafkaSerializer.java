package com.example.kafka.serializer;

import com.example.kafka.model.Message;
import lombok.extern.slf4j.Slf4j;
import org.apache.avro.Schema;
import org.apache.avro.io.DatumWriter;
import org.apache.avro.io.Encoder;
import org.apache.avro.io.EncoderFactory;
import org.apache.avro.generic.GenericData;
import org.apache.avro.generic.GenericDatumWriter;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.util.zip.GZIPOutputStream;

@Slf4j
public class AvroKafkaSerializer implements org.apache.kafka.common.serialization.Serializer<Message> {

    private final Schema schema;

    public AvroKafkaSerializer() {
        this.schema = loadSchema();
    }

    private Schema loadSchema() {
        try (InputStream is = getClass().getClassLoader().getResourceAsStream("avro/KafkaMessage.avsc")) {
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
    public byte[] serialize(String topic, Message data) {
        if (data == null) return null;

        try (ByteArrayOutputStream rawBaos = new ByteArrayOutputStream();
             ByteArrayOutputStream gzipBaos = new ByteArrayOutputStream();
             GZIPOutputStream gzip = new GZIPOutputStream(gzipBaos)) {

            // Step 1: Serialize Avro binary (no compression)
            Encoder rawEncoder = EncoderFactory.get().directBinaryEncoder(rawBaos, null);
            DatumWriter<org.apache.avro.generic.GenericRecord> writer =
                    new GenericDatumWriter<>(schema);

            GenericData.Record record = new GenericData.Record(schema);
            record.put("id", data.getId());
            record.put("topic", data.getTopic());
            record.put("key", data.getKey());
            record.put("value", data.getValue());
            record.put("partition", data.getPartition());
            record.put("offset", data.getOffset());
            record.put("timestamp", data.getTimestamp() != null ? data.getTimestamp().toEpochMilli() : 0L);

            writer.write(record, rawEncoder);
            rawEncoder.flush();
            byte[] rawBytes = rawBaos.toByteArray();

            // Step 2: GZIP compress the Avro binary
            gzip.write(rawBytes);
            gzip.finish();

            byte[] compressed = gzipBaos.toByteArray();
            double ratio = rawBytes.length > 0
                    ? (compressed.length * 100.0 / rawBytes.length)
                    : 100.0;
            log.info("[AVRO-SERIALIZE] topic={} avro={}B gzip={}B ratio={}%",
                    topic, rawBytes.length, compressed.length, String.format("%.0f", ratio));
            return compressed;

        } catch (IOException e) {
            throw new RuntimeException("Failed to serialize message for topic " + topic, e);
        }
    }

    @Override
    public void close() {
    }
}