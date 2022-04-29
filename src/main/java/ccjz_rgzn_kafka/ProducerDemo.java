package ccjz_rgzn_kafka;
import org.apache.commons.lang3.RandomStringUtils;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.StringSerializer;
import java.io.IOException;
import java.util.Properties;
public class ProducerDemo
{
    private static final String SERVERS = "master:9092,slave1:9092,salve2:9092";
    public static void main(String[] args) throws IOException
    {
        Properties pros = new Properties();
        pros.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, SERVERS);
        pros.put(ProducerConfig.ACKS_CONFIG, "all");
        pros.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        pros.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        pros.put(ProducerConfig.ENABLE_IDEMPOTENCE_CONFIG, "false");
        KafkaProducer<String, String> producer = new KafkaProducer<>(pros);
        for(int i = 0; i < 10; i++)
        {
            ProducerRecord<String, String> msg = new ProducerRecord<>("tpc_2", "name" , "1902+王家勇+2022-04-22+写入tpc_2 ") ;
            producer.send(msg);
        }
        producer.close();

    }
}
