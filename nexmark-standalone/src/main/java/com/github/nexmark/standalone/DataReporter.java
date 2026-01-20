package com.github.nexmark.standalone;

//Copyright (c) Microsoft Corporation. All rights reserved.
//Licensed under the MIT License.
import org.apache.kafka.clients.producer.Callback;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.clients.producer.RecordMetadata;
import java.util.ArrayList;

/** Note:
 *  The work in the following class involving Kafka is largely based on the Microsoft tutorial for implementing Kafka with Event Hubs.
 *  Link: https://github.com/MicrosoftDocs/azure-docs/blob/main/articles/event-hubs/event-hubs-java-get-started-send.md
 */
public class DataReporter implements Runnable {

    private final String TOPIC;
    private ArrayList<String> messages;
    private Producer<Long, String> producer;

    // Includes messages (JSON Strings of Event objects)
    public DataReporter(final Producer<Long, String> producer, String TOPIC, ArrayList<String> messages) {
        this.producer = producer;
        this.TOPIC = TOPIC;
        this.messages = messages;
    }

    @Override
    public void run() {
        // Iterates through all messages
        for(int i = 0; i < this.messages.size(); i++) {    
            
            long time = System.currentTimeMillis();
            
            // Creating a ProducerRecord of the topic ("Event"), time, and message
            ProducerRecord<Long, String> record = new ProducerRecord<Long, String>(TOPIC, time, messages.get(i));

            // Sending record
            producer.send(record, new Callback() {
                public void onCompletion(RecordMetadata metadata, Exception exception) {
                    if (exception != null) {
                        System.out.println(exception);
                        System.exit(1);
                    }
                }
            });
        }
    }
}