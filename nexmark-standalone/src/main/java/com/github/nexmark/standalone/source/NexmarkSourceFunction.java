package com.github.nexmark.standalone.source;

import com.github.nexmark.flink.generator.NexmarkGenerator;
import com.github.nexmark.flink.generator.GeneratorConfig;

import com.github.nexmark.standalone.generator.SideInputGenerator;
import com.github.nexmark.standalone.ConsumerThread;
import com.github.nexmark.standalone.DataReporter;

import java.util.ArrayList;
import java.util.concurrent.TimeUnit;

import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.common.serialization.LongSerializer;
import org.apache.kafka.common.serialization.StringSerializer;
import java.util.Properties;
import java.nio.file.FileSystems;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

public class NexmarkSourceFunction<T> {

	private final GeneratorConfig config;

	private final EventDeserializer<T> deserializer;

	private transient NexmarkGenerator generator;

	/** Flag to make the source cancelable. */
	private volatile boolean isRunning = true;

	public NexmarkSourceFunction(GeneratorConfig config, EventDeserializer<T> deserializer) {
		this.config = config;
		this.deserializer = deserializer;
	}

	// This opens a new NexmarkGenerator object based on the entered configuration
	public void open() throws Exception {
		this.generator = new NexmarkGenerator(this.config);
	}

	/** This method is intended to replace the functionality of the .execute() method used in Flink. */
	public void run(SourceContext<T> ctx) throws Exception {

		// White status isRunning is true and there are more events
		while (isRunning && generator.hasNext()) {
			long now = System.currentTimeMillis();
			NexmarkGenerator.NextEvent nextEvent = generator.nextEvent();

			if (nextEvent.wallclockTimestamp > now) {
				// sleep until wall clock less than current timestamp
				TimeUnit.SECONDS.sleep(nextEvent.wallclockTimestamp - now);
			}

			// Deserializing the next event
			T next = deserializer.deserialize(nextEvent.event);

			try {
				// Stores all of the created events - see SourceContext.java for method implementation
				ctx.collect(next);

			} catch (Exception e) {
				e.printStackTrace();
			}
		}

		// Initializes the Consumer Kafka
		// "eventobjects" is TOPIC
		consumerKafka("eventobjects");

		// Delaying time to give time for setup of Consumer Kafka
		TimeUnit time = TimeUnit.SECONDS;
		time.sleep(15);

		// Using the Producer Kafka side now
		sendKafka(ctx.jsonFormat(), "eventobjects");

		// "sideinputs" is TOPIC
		consumerKafka("sideinputs");
		time.sleep(15);

		SideInputGenerator generator = new SideInputGenerator();
		ArrayList<String> sideinput = generator.prepareSideInput(1000);
		sendKafka(sideinput, "sideinputs");
	}

	/** Note:
	 *  The work in the following work below this comment involving Kafka is largely based on the Microsoft tutorial for implementing Kafka with Event Hubs.
	 *  Link: https://github.com/MicrosoftDocs/azure-docs/blob/main/articles/event-hubs/event-hubs-java-get-started-send.md
	 */

	// Number of threads, can be adjusted as deemed necessary
    private final static int NUM_THREADS = 3;

	// Producer side of Kafka
    private static void sendKafka(ArrayList<String> messages, String topic) throws Exception {
        //Create Kafka Producer
        final Producer<Long, String> producer = createProducer();

        Thread.sleep(5000);

        final ExecutorService executorService = Executors.newFixedThreadPool(NUM_THREADS);

        for (int i = 0; i < NUM_THREADS; i++) {
            executorService.execute(new DataReporter(producer, topic, messages));
		}
    }

	// Creates Producer for Kafka
    private static Producer<Long, String> createProducer() {
        try{
            Properties properties = new Properties();

			// Insert Event Hub namespace
            properties.put("bootstrap.servers", "<insert event hub namespace>.servicebus.windows.net:9093");
			properties.put("security.protocol", "SASL_SSL");
			properties.put("sasl.mechanism", "PLAIN");
			properties.put("compression.type", "none");

            // Add Connection String in insert section by password=
			properties.put("sasl.jaas.config", "org.apache.kafka.common.security.plain.PlainLoginModule required username=\"$ConnectionString\" password=\"<insert connection string here>\";");
            properties.put(ProducerConfig.CLIENT_ID_CONFIG, "KafkaExampleProducer");
            properties.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, LongSerializer.class.getName());
            properties.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
            return new KafkaProducer<>(properties);
        } catch (Exception e){
            System.out.println("Failed to create producer with exception: " + e);
            System.exit(0);
            return null;
        }
    }

	// Initializes the consumer end of Kafka
    public static void consumerKafka(String topic) throws Exception {

		// Uses the same number of threads as Producer
        final ExecutorService executorService = Executors.newFixedThreadPool(NUM_THREADS);

		// Again, preserving this structure to allow for greater flexibility in the future should the number of threads be increased
        for (int i = 0; i < NUM_THREADS; i++) {
            executorService.execute(new ConsumerThread(topic));
        }
    }
}
