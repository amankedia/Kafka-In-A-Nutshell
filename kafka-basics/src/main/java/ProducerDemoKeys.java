import org.apache.kafka.clients.producer.*;
import org.apache.kafka.common.serialization.StringSerializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Properties;
import java.util.concurrent.ExecutionException;

/**
 * Created by amankedia on 7/26/19.
 */
public class ProducerDemoKeys {
    public static void main(String[] args) throws InterruptedException, ExecutionException {


        Logger logger = LoggerFactory.getLogger(ProducerDemoWithCallback.class);
        // Producer properties

        String bootstrapServers = "127.0.0.1:9092";
        Properties properties = new Properties();
        properties.setProperty(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers);
        properties.setProperty(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        properties.setProperty(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());

        // Producer

        KafkaProducer<String, String> producer = new KafkaProducer<String, String>(properties);

        // Create producer record
        for (int i=0; i<10; i++) {

            String topic = "first_topic";
            String value = Integer.toString(i);
            String key = "id_" + value;

            ProducerRecord<String, String> record = new ProducerRecord<String, String>(topic, key, value);

            logger.info("Key: "+ key);

            // Send the data
            producer.send(record, new Callback() {
                @Override
                public void onCompletion(RecordMetadata metadata, Exception exception) {
                    //executes everything message is sent or exception is thrown
                    if (exception == null) {
                        logger.info("Received new metadata\n" +
                                "Topic:" + metadata.topic() + "\n" +
                                "Partition:" + metadata.partition() + "\n" +
                                "Offset:" + metadata.offset() + "\n" +
                                "Timestamp:" + metadata.timestamp());
                    } else {
                        logger.error("Error while producing", exception);
                    }
                }
            }).get(); //block the send to make it synchronous, never do in production
        }

        producer.flush();
        producer.close();

    }
}
