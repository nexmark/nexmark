package com.github.nexmark.standalone;

//Copyright (c) Microsoft Corporation. All rights reserved.
//Licensed under the MIT License.
import org.apache.kafka.clients.consumer.*;
import org.apache.kafka.common.serialization.LongDeserializer;
import org.apache.kafka.common.serialization.StringDeserializer;
import java.time.Duration;
import java.util.Collections;
import java.util.Properties;
import java.util.concurrent.atomic.AtomicInteger;

/** Note:
 *  The work in the following class involving Kafka is largely based on the Microsoft tutorial for implementing Kafka with Event Hubs.
 *  Link: https://github.com/MicrosoftDocs/azure-docs/blob/main/articles/event-hubs/event-hubs-java-get-started-send.md
 */
public class ConsumerThread implements Runnable {

    private final String TOPIC;

    // Initializes ConsumerThread object
    public ConsumerThread(final String TOPIC){
        this.TOPIC = TOPIC;
    }

    @Override
    public void run() {
        final Consumer<Long, String> consumer = createConsumer();

        try {
            while (true) {
                // Includes updated syntax for consumer.poll() function which is depreciated if not used with Duration
                final ConsumerRecords<Long, String> consumerRecords = consumer.poll(Duration.ofSeconds(1000));

                // Iterating through consumerRecords
                for(ConsumerRecord<Long, String> cr : consumerRecords) {
                    /** Example of Statement:
                     * Consumer Record:(1687819270433, "Auction{id=1211, itemName='dwoiiuq', description='hvqcartuxyddypkwrmusuogyvrwaesotztq dowirbjcaqsqczyrfambzwiwdfx', initialBid=939, reserve=1083, dateTime=2023-06-26T22:40:03.475Z, expires=2023-06-26T22:40:03.485Z, seller=1032, category=10, extra='ixhimsRQY]JSSH]VX`TSVISMTV]YUS_YHMJIMLLUWUezzbffvrzabqKURUaMbqlbyynnuwriwaocgwddweci^]\\NYJqqvfilVWV]ZSjlyflaINHZ`[SVMV^PP_SUW]X^YPV[]^WKTQnkznjhHIKWVUjozcduV_[_aaJVSX_Qkebsymaugnwhngtdvz`LYaIWzxaptv^NaRMafbudhnniwqgySQN\\SHLa][`aK\\KYJ[kaajfjK`XP^KkjabghUY[WLItjhwweHXYLXWywtxreuqhnrdrhnvzcsekcyriadunsSa_XONVaNaKPyewghxNVPJ^LOIIYJXUPX\\\\Kwudfkwtywofwbtydye\\TLXa`jxdkybTLRWN_T]M`KVrzwendRR^HaXykytdwQLVH`aevwmwiMP]YNKo'}", 0, 8102)
                     */
                    System.out.printf("Consumer Record:(%d, %s, %d, %d)\n", cr.key(), cr.value(), cr.partition(), cr.offset());
                }
                consumer.commitAsync();
            }
        } catch (CommitFailedException e) {
            System.out.println("CommitFailedException: " + e);
        } finally {
            consumer.close();
        }
    }

    // Creating the Consumer end of Kafka
    private Consumer<Long, String> createConsumer() {
        try {
            final Properties properties = new Properties();

            AtomicInteger id = new AtomicInteger(0);
            properties.put(ConsumerConfig.CLIENT_ID_CONFIG, "KafkaExampleConsumer#" + id.getAndIncrement());

            // Setting up the properties for the Kafka Consumer

            // Insert event hub namespace here
            properties.put("bootstrap.servers", "<insert event hub namespace>.servicebus.windows.net:9093");
            properties.put("security.protocol", "SASL_SSL");
            properties.put("sasl.mechanism", "PLAIN");

            // Insert connection string where it says to, by password=
            properties.put("sasl.jaas.config", "org.apache.kafka.common.security.plain.PlainLoginModule required username=\"$ConnectionString\" password=\"<insert connection string>\";");
            properties.setProperty(ConsumerConfig.GROUP_ID_CONFIG, "nexmarkdata");
            properties.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, LongDeserializer.class.getName());
            properties.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());

            // Create the consumer using properties.
            Consumer<Long, String> consumer = new KafkaConsumer<>(properties);

            // Subscribe to the topic.
            consumer.subscribe(Collections.singletonList(TOPIC));
            return consumer;
            
        } catch (Exception e){
            System.out.println("Exception: " + e);
            System.exit(1);

            return null;        
        }
    }
}
