package demo.kafka;

import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.apache.kafka.common.serialization.StringSerializer;
import org.springframework.beans.factory.annotation.Value;

import java.util.Arrays;
import java.util.Properties;

public class MasterConsumer {


    private static KafkaConsumer<String, String> consumer;

    private MasterConsumer(){

    }

    public static KafkaConsumer<String, String> getConsumer(String brokers, String topic){

        if(consumer == null ){
            System.out.println("null");
            Properties props = new Properties();
            String jaasTemplate = "org.apache.kafka.common.security.scram.ScramLoginModule required username=\"%s\" password=\"%s\";";
            String jaasCfg = String.format(jaasTemplate, "xikakmeq", "8lqq2FoITX3oeumIeELJ61YZzANZwgPn");
            String deserializer = StringDeserializer.class.getName();
            props = new Properties();
            props.put("bootstrap.servers", brokers);
            props.put("group.id", "xikakmeq-consumer");
            props.put("enable.auto.commit", "true");
            props.put("auto.commit.interval.ms", "1000");
            props.put("auto.offset.reset", "earliest");
            props.put("session.timeout.ms", "30000");
            props.put("key.deserializer", deserializer);
            props.put("value.deserializer", deserializer);
            props.put("security.protocol", "SASL_SSL");
            props.put("sasl.mechanism", "SCRAM-SHA-256");
            props.put("sasl.jaas.config", jaasCfg);

            consumer = new KafkaConsumer<>(props);
            consumer.subscribe(Arrays.asList(topic));

            return consumer;
        }
        System.out.println("no es nullo ==========================");
        return consumer;
    }




}
