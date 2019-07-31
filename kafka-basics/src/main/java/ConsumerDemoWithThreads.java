import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.errors.WakeupException;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.time.Duration;
import java.util.Arrays;
import java.util.Properties;
import java.util.concurrent.CountDownLatch;

/**
 * Created by amankedia on 7/26/19.
 */
public class ConsumerDemoWithThreads {

    public static void main (String[] args) throws InterruptedException {
        new ConsumerDemoWithThreads().run();
    }

    private ConsumerDemoWithThreads(){

    }

    private void run() throws InterruptedException {

        Logger logger = LoggerFactory.getLogger(ConsumerDemoWithThreads.class.getName());

        String bootstrapServers = "127.0.0.1:9092";
        String groupId = "my_second_application";
        String topic = "first_topic";

        CountDownLatch latch = new CountDownLatch(1);
        logger.info("Creating consumer thread");
        ConsumerRunnable myConsumerRunnable = new ConsumerRunnable(latch, topic, bootstrapServers, groupId);

        Thread myThread = new Thread(myConsumerRunnable);
        myThread.start();

        Runtime.getRuntime().addShutdownHook(new Thread( () -> {
            logger.info("Caught shutdown group");
            myConsumerRunnable.shutdown();
        }
        ));
        try {
            latch.await();
        } catch (InterruptedException e){
            logger.error("Application got interrupted "+e );
        } finally {
            logger.info("Application is closing");
        }

    }

    public class ConsumerRunnable implements Runnable {

        private CountDownLatch latch;
        private KafkaConsumer<String , String> consumer;
        private Logger logger = LoggerFactory.getLogger(ConsumerRunnable.class.getName());

        public ConsumerRunnable(CountDownLatch latch, String topic, String bootstrapServers, String groupId){

            // consumer config
            Properties properties = new Properties();
            properties.setProperty(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers);
            properties.setProperty(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
            properties.setProperty(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
            properties.setProperty(ConsumerConfig.GROUP_ID_CONFIG, groupId);
            properties.setProperty(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");
            this.latch = latch;
            consumer = new KafkaConsumer<String, String>(properties);
            consumer.subscribe(Arrays.asList(topic));
        }

        @Override
        public void run() {
            try {
                while(true){
                    ConsumerRecords<String, String> records = consumer.poll(Duration.ofMillis(100));

                    for (ConsumerRecord record: records){
                        logger.info("Received new message\n" +
                                "Topic:" + record.topic() + "\n" +
                                "Partition:" + record.partition() + "\n" +
                                "Offset:" + record.offset() + "\n" +
                                "Timestamp:" + record.timestamp());
                    }
                }
            } catch (WakeupException e){
                logger.info("Info received shutdown signal");
            } finally {
                consumer.close();
                //tell our main code that we are done with consumer
                latch.countDown();
            }
        }

        public void shutdown(){
            //wakeup is a special method to intrerrupt.poll. It will throw a Wakeup exception
            consumer.wakeup();
        }

        public void b(){
            System.out.println("OK");
        }
    }
}
