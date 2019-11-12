package demo.kafka.controllers;

import demo.kafka.MasterConsumer;
import demo.kafka.Producer;
import demo.kafka.model.Coin;
import demo.kafka.producers.UserProducer;
import demo.kafka.responseFormat.ResponseText;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.kafka.KafkaException;
import org.springframework.web.bind.annotation.PathVariable;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestMethod;
import org.springframework.web.bind.annotation.RestController;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.web.client.RestTemplate;

@RestController
@RequestMapping("user")
public class UserController {

    @Autowired
    Producer producer;

    @RequestMapping(value = "",method = RequestMethod.GET)
    public ResponseText index() {
        return new ResponseText("API usuarios :: working...  [kafka producer + Spring Boot] ");
    }
    @RequestMapping("/send/{msg}")
    public ResponseText message(@PathVariable String msg){
        final int minBatchSize = 200;
        List<ConsumerRecord<String, String>> buffer = new ArrayList<>();
        try{
            // using producer
            long offset = producer.send(msg);

            KafkaConsumer<String, String> consumer =  MasterConsumer.getConsumer(producer.getBrokers(),producer.getTopic());

            boolean running =true;

            while (running) {
                    ConsumerRecords<String, String> records = consumer.poll(100);
                    for (ConsumerRecord<String, String> record : records) {
                        System.out.println(record.offset() + ": " + record.value());
                        if(record.offset()  ==offset )
                            return new ResponseText( record.value(),offset );
                    }

            }

        }
        catch (KafkaException e){
            System.out.println("Error: ");
            System.out.println(e.getMessage());
        }finally {
           // consumer.close();
        }
        return new ResponseText( "error: not working madafaka" );
    }

    @RequestMapping("/coin")
    public Coin[] c(){

        RestTemplate restTemplate = new RestTemplate();
        Coin[] coins = restTemplate.getForObject("https://api.coinmarketcap.com/v1/ticker/", Coin[].class);


        return coins;
    }
    @RequestMapping("/hi")
    public String hi(){

        RestTemplate restTemplate = new RestTemplate();
        ResponseText hi= restTemplate.getForObject("https://hola-mundo-node.herokuapp.com/", ResponseText.class);
        return hi.getResponse();
    }
}
