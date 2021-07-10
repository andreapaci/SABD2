package it.sabd.uniroma2.app.kafka;


import it.sabd.uniroma2.app.util.Constants;
import org.apache.kafka.clients.admin.AdminClient;
import org.apache.kafka.clients.admin.NewTopic;
import org.apache.kafka.clients.consumer.Consumer;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.TopicPartition;

import java.time.Duration;
import java.util.*;
import java.util.concurrent.TimeUnit;


//Access point to Kafka Cluster as Singleton
public class KafkaClient {

    private static KafkaClient instance;

    private Properties properties;
    private AdminClient adminClient;
    private Producer<Long, String> kafkaProducer;
    private KafkaConsumer<String, String> kafkaConsumer;


    private KafkaClient(){

        properties = new Properties();

        properties.put("bootstrap.servers", Constants.KAFKA_HOSTS);
        properties.put("group.id", "client");
        properties.put("enable.auto.commit", "true");
        properties.put("auto.commit.interval.ms", "1000");
        properties.put("key.serializer", "org.apache.kafka.common.serialization.LongSerializer");
        properties.put("value.serializer", "org.apache.kafka.common.serialization.StringSerializer");
        properties.put("key.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");
        properties.put("value.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");


        Properties propertiesClient = new Properties();

        propertiesClient.put("bootstrap.servers", Constants.KAFKA_HOSTS);
        propertiesClient.put("group.id", "consumer");
        propertiesClient.put("key.serializer", "org.apache.kafka.common.serialization.LongSerializer");
        propertiesClient.put("value.serializer", "org.apache.kafka.common.serialization.StringSerializer");
        propertiesClient.put("key.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");
        propertiesClient.put("value.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");

        kafkaProducer = new KafkaProducer<>(properties);
        kafkaConsumer = new KafkaConsumer<>(propertiesClient);
    }

    public void addInputTopic(){
        addNewTopic(Constants.INPUT_TOPIC_NAME, Constants.PARTITION_NUMBER, Constants.REPLICATION_FACTOR);
    }

    public void addOutputTopic(){

        addNewTopic(Constants.OUTPUT_TOPIC_NAME, Constants.PARTITION_NUMBER, Constants.REPLICATION_FACTOR);

        try { TimeUnit.SECONDS.sleep(10L); }
        catch (Exception e) { System.out.println("Could not wait for the Consumer to Subscribe..."); e.printStackTrace(); }

        System.out.println("Subscribing to " + Constants.OUTPUT_TOPIC_NAME);
        kafkaConsumer.subscribe(Collections.singletonList(Constants.OUTPUT_TOPIC_NAME));

        try { TimeUnit.SECONDS.sleep(10L); }
        catch (Exception e) { System.out.println("Could not wait for the Consumer to Subscribe..."); e.printStackTrace(); }
    }

    private void addNewTopic(String topicName, int numberPartition, int replicationFactor){

        adminClient = AdminClient.create(properties);

        NewTopic newTopic = new NewTopic(topicName, numberPartition, (short) replicationFactor); //new NewTopic(topicName, numPartitions, replicationFactor)

        List<NewTopic> newTopics = new ArrayList<NewTopic>();
        newTopics.add(newTopic);

        adminClient.createTopics(newTopics);

        adminClient.close();

    }

    public void sendMessage(long key, String value){

        ProducerRecord<Long, String> record =
                new ProducerRecord<>(Constants.INPUT_TOPIC_NAME, 0, key, key, value);

        //Send asynchronous
        kafkaProducer.send(record, (metadata, exception) -> {
            if (metadata == null) {
                System.out.println("Error sending message: [" + record.key() + "] " + record.value());
                exception.printStackTrace();
            }

        });

        kafkaProducer.flush();

    }

    public ArrayList<String> readMessage(){


        ArrayList<String> texts = new ArrayList<>();

        ConsumerRecords<String, String> records = kafkaConsumer.poll(Duration.ofSeconds(1));

        records.forEach(record -> {
            texts.add(record.value());
        });

        kafkaConsumer.commitAsync();

        return texts;
    }

    public void killProducer(){
        kafkaProducer.close();
    }

    public static KafkaClient getInstance(){
        if(instance == null)
            instance = new KafkaClient();
        return instance;
    }

}
