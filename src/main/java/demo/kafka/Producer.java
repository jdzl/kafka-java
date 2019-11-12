/*
 * Copyright 2012-2018 the original author or authors.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package demo.kafka;


import demo.kafka.model.Usuario;
import demo.kafka.responseFormat.ResponseText;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.apache.kafka.common.serialization.StringSerializer;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.kafka.support.SendResult;
import org.springframework.kafka.support.serializer.JsonSerializer;
import org.springframework.stereotype.Component;
import org.springframework.util.concurrent.ListenableFuture;
import org.springframework.util.concurrent.ListenableFutureCallback;
import org.springframework.web.client.RestTemplate;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Arrays;
import java.util.HashMap;
import java.util.Map;
import java.util.Properties;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;

@Component
public class Producer {

    private static final Logger LOGGER = LoggerFactory.getLogger(Producer.class);
    private final KafkaTemplate<String, String> kafkaTemplate;
 //   private final KafkaConsumer<String, String> consumer;



    RestTemplate restTemplate ;


    @Value("${cloudkarafka.topic}")
    private String topic;

    @Value("${spring.kafka.bootstrap-servers}")
    private String brokers;

    Producer(KafkaTemplate<String, String> kafkaTemplate) {
        this.kafkaTemplate = kafkaTemplate;
   //     this.consumer = consumer;
        restTemplate = new RestTemplate();
    }


    public long send(String id){

        ResponseText hi= restTemplate.getForObject("https://hola-mundo-node.herokuapp.com/", ResponseText.class);
        ResponseText world= restTemplate.getForObject("https://hola-mundo-python.herokuapp.com/", ResponseText.class);

        Usuario usr = restTemplate.getForObject("https://user-info-node-test.herokuapp.com/users/1545631", Usuario.class);
    //   ResponseText infoBank= restTemplate.getForObject("https://info-bancaria.herokuapp.com/infobank"+usr.getAccount(), ResponseText.class);
        System.out.println(usr.getAddress());
        System.out.println(usr.getFirstName());
        System.out.println(usr.getAccountNumber());
        System.out.println(usr.getAccountNumber());






        String alterMessage = id + " "+ hi.getResponse() + " " + world.getResponse();;
        ListenableFuture<SendResult<String, String>> sendResult =  this.kafkaTemplate.send(topic,alterMessage);
        try {
        SendResult<String, String> sendResult2 = sendResult.get(10, TimeUnit.SECONDS);
        return  sendResult2.getRecordMetadata().offset();
        }
        catch (InterruptedException e) {
            // TODO Auto-generated catch block
            e.printStackTrace();
            Thread.currentThread().interrupt();
        }
        catch (ExecutionException e) {
            // TODO Auto-generated catch block
            e.printStackTrace();
        }
        catch (TimeoutException e) {
            // TODO Auto-generated catch block
            e.printStackTrace();
        }

        /*
        // register a callback with the listener to receive the result of the send asynchronously
        sendResult.addCallback(new ListenableFutureCallback<SendResult<String, String>>() {

            @Override
            public void onSuccess(SendResult<String, String> result) {
                long offset = result.getRecordMetadata().offset();
                LOGGER.info("sent message='{}' with offset={}", message,  result.getRecordMetadata().offset());

               KafkaConsumer<String, String> consumer =  MasterConsumer.getConsumer(brokers,topic);
                consumer.subscribe(Arrays.asList(topic));

                boolean running =true;
                try {
                   while (running) {
                System.out.println("================================================================");
                    ConsumerRecords<String, String> records = consumer.poll(100);
                        for (ConsumerRecord<String, String> record : records) {
                            System.out.println(record.offset() + ": " + record.value());
                            if(record.offset()  == result.getRecordMetadata().offset()){
                                System.out.println(record.offset()+"este es:::::::::::"+result.getRecordMetadata().offset());
                                running = false;
                                break;
                            }
                        }
                    System.out.println("================================================================");
                    }
                } finally {
                    consumer.close();
                }

            }

            @Override
            public void onFailure(Throwable ex) {
                LOGGER.error("unable to send message='{}'", message, ex);
            }
        });
*/
        System.out.println("Sent simple message [" + id + "] to " + topic);
        return  0;
    }


    public void send(SampleMessage message) {
        this.kafkaTemplate.send(topic, message.getMessage());
        System.out.println("Sent message [" + message + "] to " + topic);
    }

    public String getTopic(){
        return  topic;
    }

    public String getBrokers() {
        return brokers;
    }

    public void setBrokers(String brokers) {
        this.brokers = brokers;
    }
}
