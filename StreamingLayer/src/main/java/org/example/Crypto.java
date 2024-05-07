package org.example;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.apache.kafka.common.serialization.StringSerializer;
import org.bson.Document;

import com.mongodb.client.MongoClient;
import com.mongodb.client.MongoClients;
import com.mongodb.client.MongoCollection;
import com.mongodb.client.MongoDatabase;

import java.io.BufferedReader;
import java.io.InputStreamReader;
import java.io.IOException;
import java.util.Arrays;
import java.util.Properties;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;

public class Crypto {
    private static final String TOPIC_NAME = "connect-test";
    private static final String BOOTSTRAP_SERVERS = "localhost:9092";
    private static final String MONGO_DATABASE_NAME = "crypto_trades";
    private static final String MONGO_COLLECTION_NAME = "crypto";

    public static void main(String[] args) throws IOException {
        if (args.length != 1) {
            System.err.println("Usage: Crypto <hdfs-file-path>");
            System.exit(1);
        }

        String hdfsFilePath = args[0];

        // Kafka producer properties
        Properties producerProperties = new Properties();
        producerProperties.put("bootstrap.servers", BOOTSTRAP_SERVERS);
        producerProperties.put("key.serializer", StringSerializer.class.getName());
        producerProperties.put("value.serializer", StringSerializer.class.getName());

        // Kafka consumer properties
        Properties consumerProperties = new Properties();
        consumerProperties.put("bootstrap.servers", BOOTSTRAP_SERVERS);
        consumerProperties.put("group.id", "test-consumer-group");
        consumerProperties.put("enable.auto.commit", "true");
        consumerProperties.put("auto.commit.interval.ms", "1047");
        consumerProperties.put("key.deserializer", StringDeserializer.class.getName());
        consumerProperties.put("value.deserializer", StringDeserializer.class.getName());

        ExecutorService executorService = Executors.newFixedThreadPool(2);

        executorService.submit(() -> {
            try {
                produceToKafka(producerProperties, hdfsFilePath);
            } catch (IOException | InterruptedException e) {
                e.printStackTrace();
            }
        });

        executorService.submit(() -> consumeFromKafka(consumerProperties));

        executorService.shutdown();
    }

    private static void produceToKafka(Properties properties, String hdfsFilePath) throws IOException, InterruptedException {
        try (Producer<String, String> producer = new KafkaProducer<>(properties);
             FileSystem fs = FileSystem.get(new Configuration());
             FSDataInputStream inputStream = fs.open(new Path(hdfsFilePath));
             BufferedReader reader = new BufferedReader(new InputStreamReader(inputStream))) {
            String line;
            while ((line = reader.readLine()) != null) {
                System.out.println("Read line: " + line);

                ProducerRecord<String, String> record = new ProducerRecord<>(TOPIC_NAME, null, line);
                producer.send(record);

                TimeUnit.MILLISECONDS.sleep(500); // Delay of 0.5 minutes (500 milliseconds)
            }
        }
    }

    private static void consumeFromKafka(Properties properties) {
        try (KafkaConsumer<String, String> consumer = new KafkaConsumer<>(properties);
             MongoClient mongoClient = MongoClients.create("mongodb://localhost:27017")) {
            consumer.subscribe(Arrays.asList(TOPIC_NAME));
            MongoDatabase database = mongoClient.getDatabase(MONGO_DATABASE_NAME);
            MongoCollection<Document> collection = database.getCollection(MONGO_COLLECTION_NAME);

            while (true) {
                ConsumerRecords<String, String> records = consumer.poll(30000);
                for (ConsumerRecord<String, String> record : records) {
                    System.out.println(record.value());

                    // Insert record value into MongoDB
                    Document doc = new Document("value", record.value());
                    collection.insertOne(doc);
                    System.out.println("Document inserted into MongoDB");
                }
            }
        }
    }
}
