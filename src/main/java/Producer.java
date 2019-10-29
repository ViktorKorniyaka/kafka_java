import org.apache.kafka.clients.producer.*;
import org.apache.kafka.common.serialization.StringSerializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Properties;
import java.util.concurrent.ExecutionException;

class Producer {

  public static void main(String[] args) throws ExecutionException, InterruptedException {
    String server = "127.0.0.1:9092";
    String topic = "user_registered";

    Producer producer = new Producer(server);
    producer.put(topic, "user1", "John1");
    producer.put(topic, "user2", "Peter2");
    producer.put(topic, "user3", "John3");
    producer.put(topic, "user4", "Peter4");
    producer.close();
  }

  // Variables

  private final KafkaProducer<String, String> mProducer;
  private final Logger mLogger = LoggerFactory.getLogger(Producer.class);

  // Constructors

  Producer(String bootstrapServer) {
    Properties props = producerProps(bootstrapServer);
    mProducer = new KafkaProducer<>(props);

    mLogger.info("Producer initialized");
  }

  // Public

  void put(String topic, String key, String value) throws ExecutionException, InterruptedException {
    mLogger.info("Put value: " + value + ", for key: " + key);

    ProducerRecord<String, String> record = new ProducerRecord<>(topic, key, value);
    mProducer.send(record, (recordMetadata, e) -> {
      if (e != null) {
        mLogger.error("Error while producing", e);
        return;
      }

      mLogger.info("Received new meta. Topic: " + recordMetadata.topic()
          + "; Partition: " + recordMetadata.partition()
          + "; Offset: " + recordMetadata.offset()
          + "; Timestamp: " + recordMetadata.timestamp());
    }).get();
  }

  void close() {
    mLogger.info("Closing producer's connection");
    mProducer.close();
  }

  // Private

  private Properties producerProps(String bootstrapServer) {
    String serializer = StringSerializer.class.getName();
    Properties props = new Properties();
    props.setProperty(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServer);
    props.setProperty(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, serializer);
    props.setProperty(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, serializer);
    //props.setProperty(ProducerConfig.ENABLE_IDEMPOTENCE_CONFIG, "true");
    //advertised.listeners=PLAINTEXT://127.0.0.1:9092

    return props;
  }
}
