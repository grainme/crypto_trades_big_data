package org.example;

import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.ProducerRecord;

import java.io.BufferedReader;
import java.io.FileReader;
import java.io.IOException;
import java.util.Properties;

public class TestTxtToKafkaProducer {

    private static final String TOPIC_NAME = "connect-test";
    private static final String FILE_PATH = "/home/hadoop/kafka_2.13-3.7.0/test.txt";
    private static final String BOOTSTRAP_SERVERS = "localhost:9092";

    public static void main(String[] args) throws IOException {
        // Kafka producer configuration
        Properties properties = new Properties();
        properties.put("bootstrap.servers", BOOTSTRAP_SERVERS);
        properties.put("key.serializer", "org.apache.kafka.common.serialization.StringSerializer");
        properties.put("value.serializer", "org.apache.kafka.common.serialization.StringSerializer");

        // Create Kafka producer
        Producer<String, String> producer = new KafkaProducer<>(properties);

        // Read records from test.txt and produce them to Kafka topic
        try (BufferedReader reader = new BufferedReader(new FileReader(FILE_PATH))) {
            String line;
            while ((line = reader.readLine()) != null) {
                // Create a ProducerRecord and send it to the Kafka topic
                ProducerRecord<String, String> record = new ProducerRecord<>(TOPIC_NAME, null, line);
                System.out.println(line);
                producer.send(record);
            }
        }

        // Close the Kafka producer
        producer.close();
    }
}
