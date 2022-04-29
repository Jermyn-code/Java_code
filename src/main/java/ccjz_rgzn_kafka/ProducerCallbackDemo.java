package ccjz_rgzn_kafka;

import org.apache.commons.lang3.RandomStringUtils;
import org.apache.kafka.clients.producer.*;
import org.apache.kafka.common.serialization.StringSerializer;

import java.io.IOException;
import java.util.Properties;

public class ProducerCallbackDemo {
    private static final String SERVERS = "master:9092,slave1:9092,salve2:9092";

    public static void main(String[] args) throws IOException {

        Properties pros = new Properties();
        pros.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, SERVERS);
        pros.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        pros.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());

        KafkaProducer<String, String> producer = new KafkaProducer<>(pros);

        for (int i = 0; i < 10; i++) {
            ProducerRecord<String, String> rcd = new ProducerRecord<String, String>("tpc_1", "key" + i, "rgzn_bigdata-WJY_2022-04-20 " );
            producer.send(rcd, new Callback() {
                @Override
                public void onCompletion(RecordMetadata recordMetadata, Exception e) {
                    //如果响应是成功的，则recordMetadata是有值的
                    if (recordMetadata != null) {
                        System.out.println(recordMetadata.topic());
                        System.out.println(recordMetadata.offset());
                        System.out.println(recordMetadata.serializedKeySize());
                        System.out.println(recordMetadata.serializedValueSize());
                        System.out.println(recordMetadata.timestamp());
                    }
                    ;
                }
            });
        }
        producer.close();
    }

}
