package demo.kafka.producers;

import demo.kafka.model.User;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.kafka.core.KafkaTemplate;

public class UserProducer {


    @Value("${cloudkarafka.topicUser}")
    private String topic;

    private final KafkaTemplate<String, User> kafkaTemplate;

    public UserProducer(KafkaTemplate<String, User> kafkaTemplate) {
        this.kafkaTemplate = kafkaTemplate;
    }

    public void send(User usr){

        this.kafkaTemplate.send(topic,usr);
        System.out.println("Sent complex  message [ user::*] to " + topic);
    }
}
