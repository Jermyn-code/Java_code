package ccjz_rgzn_kafka;

import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;

import java.time.Duration;
import java.util.Arrays;
import java.util.Properties;
import java.util.concurrent.atomic.AtomicBoolean;

public class Consumer_Task implements Runnable{
    Properties props=null;
    AtomicBoolean isRunning=null;
    public Consumer_Task(Properties props,AtomicBoolean isRunning){
        this.props=props;
        this.isRunning=isRunning;
    }
    @Override
    public void run() {
        KafkaConsumer<String, String> consumer = new KafkaConsumer<String, String>(props);

        consumer.subscribe(Arrays.asList("tpc_1"));


        ConsumerRecords<String, String> records = consumer.poll(Duration.ofMillis(Long.MAX_VALUE));
        for (ConsumerRecord<String, String> record : records) {
            //do some process做一些处理
            System.out.println(record.key()+","
                    +record.value()+","
                    +record.topic()+","
                    +record.partition()+","
                    +record.offset());
            System.out.println("---------------------------------------------------");

        }
    }
//    consumer.close(Duration.ofMillis(1000));

}


