package org.example;

import com.mongodb.ConnectionString;
import com.mongodb.MongoClientSettings;
import com.mongodb.client.MongoClient;
import com.mongodb.client.MongoClients;
import com.mongodb.client.MongoCollection;
import com.mongodb.client.MongoDatabase;
import org.apache.spark.SparkConf;
import org.apache.spark.streaming.Duration;
import org.apache.spark.streaming.api.java.*;
import org.apache.spark.streaming.kafka.KafkaUtils;
import org.bson.Document;

import scala.Tuple2;

import java.util.*;

class Record {
    private long timestamp;
    private int id;
    private double vwap;
    private double low;
    private double high;
    private double count;

    public Record(long timestamp, int id, double vwap, double high , double  low , double count) {
        this.timestamp = timestamp;
        this.id = id;
        this.vwap = vwap;
        this.high = high ;
        this.low = low ;
        this.count = count;
    }

    public double getCount() {
        return count;
    }

    public double getLow() {
        return low;
    }

    public double getHigh() {
        return high;
    }

    public long getTimestamp() {
        return timestamp;
    }

    public int getId() {
        return id;
    }


    public double getVwap() {
        return vwap;
    }
}
public class SparkKafkaWordCount {

    public static void main(String[] args) throws Exception {
        SparkConf sparkConf = new SparkConf()
                .setAppName("SparkKafkaWordCount")
                .setMaster("local[*]");

        JavaStreamingContext jssc = new JavaStreamingContext(sparkConf, new Duration(1000));

        String zkQuorum = "localhost:2181";
        String group = "group";
        String topic = "crypto_f";
        int numThreads = 1;

        Map<String, Integer> topicMap = new HashMap<>();
        topicMap.put(topic, numThreads);

        try {
            JavaPairReceiverInputDStream<String, String> messages =
                    KafkaUtils.createStream(jssc, zkQuorum, group, topicMap);

            System.out.println(messages);
            JavaDStream<String> lines = messages.map(Tuple2::_2);

            JavaDStream<String> individualLines = lines.flatMap(batch -> Arrays.asList(batch.split("\n")).iterator());

            individualLines.foreachRDD(rdd -> {
                System.out.println("Number of records in this RDD: " + rdd.count());
                rdd.foreach(line -> System.out.println("Individual Line: " + line));
            });

            JavaDStream<Record> records = individualLines.map(line -> {
                String[] word = line.split(",");
                long timestamp = Long.parseLong(word[0].trim());
                int id = Integer.parseInt(word[1].trim());
                double count = Double.parseDouble(word[2].trim());
                double high = Double.parseDouble(word[4].trim());
                double low = Double.parseDouble(word[5].trim());
                double vwap = Double.parseDouble(word[8].trim());

                return new Record(timestamp, id, vwap, high, low, count);
            });

            records.print();

            jssc.start();
            jssc.awaitTermination();
        } catch (Exception e) {
            e.printStackTrace();
        }

    }
}