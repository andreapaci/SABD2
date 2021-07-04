package it.sabd.uniroma2.kafkaclient;


import org.apache.kafka.clients.admin.AdminClient;
import org.apache.kafka.clients.admin.NewTopic;
import org.apache.kafka.clients.consumer.Consumer;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.ProducerRecord;

import java.util.ArrayList;
import java.util.List;
import java.util.Properties;


//Access point to Kafka Cluster as Singleton
public class KafkaClient {

    private static KafkaClient instance;

    private Properties properties;
    private AdminClient adminClient;
    private Producer<String, String> kafkaProducer;
    private Consumer<String, String> kafkaConsumer;


    private KafkaClient(){

        properties = new Properties();


        properties.put("bootstrap.servers", Constants.KAFKA_HOSTS);
        properties.put("group.id", "client");
        properties.put("enable.auto.commit", "true");
        properties.put("auto.commit.interval.ms", "1000");
        properties.put("key.serializer", "org.apache.kafka.common.serialization.StringSerializer");
        properties.put("value.serializer", "org.apache.kafka.common.serialization.StringSerializer");
        properties.put("key.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");
        properties.put("value.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");


        kafkaProducer = new KafkaProducer<>(properties);
        kafkaConsumer = new KafkaConsumer<>(properties);
    }

    public void addInputTopic(){
        addNewTopic(Constants.INPUT_TOPIC_NAME, Constants.PARTITION_NUMBER, Constants.REPLICATION_FACTOR);
    }

    public void addOutputTopic(){
        addNewTopic(Constants.OUTPUT_TOPIC_NAME, Constants.PARTITION_NUMBER, Constants.REPLICATION_FACTOR);
    }

    public void addNewTopic(String topicName, int numberPartition, int replicationFactor){

        adminClient = AdminClient.create(properties);

        NewTopic newTopic = new NewTopic(topicName, numberPartition, (short) replicationFactor); //new NewTopic(topicName, numPartitions, replicationFactor)

        List<NewTopic> newTopics = new ArrayList<NewTopic>();
        newTopics.add(newTopic);

        adminClient.createTopics(newTopics);

        adminClient.close();

    }

    public void sendMessage(long key, String value){

        ProducerRecord<String, String> record =
                new ProducerRecord<>(Constants.INPUT_TOPIC_NAME, Long.toString(key), value);

        //Send asynchronous
        kafkaProducer.send(record, (metadata, exception) -> {
            if (metadata == null) {
                System.out.println("Error sending message: [" + record.key() + "] " + record.value());
                exception.printStackTrace();
            }

        });

        kafkaProducer.flush();

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
