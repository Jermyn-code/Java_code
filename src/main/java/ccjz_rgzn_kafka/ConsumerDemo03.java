package ccjz_rgzn_kafka;

import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.protocol.types.Field;
import org.apache.kafka.common.serialization.StringDeserializer;

import java.time.Duration;
import java.util.Arrays;
import java.util.List;
import java.util.Properties;
import java.util.concurrent.atomic.AtomicBoolean;

public class ConsumerDemo03 {
    private static final String SERVERS = "master:9092,slave1:9092,slave2:9092";

    public static void main(String[] args) throws InterruptedException {

        //定义一个AtomicBoolean类型的isRunning来控制消费者拉取消息
        AtomicBoolean isRunning = new AtomicBoolean(true);

        //1.参数配置
        Properties props = new Properties();
        //key的反序列化器
        props.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        //value的反序列化器
        props.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        //服务器地址
        props.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, SERVERS);
        //设置自动读取的起始offset（偏移量），值可以是：earliest，latest，none
        props.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");
        //设置自动提交offset（偏移量）
        props.put(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, true);
        //设置消费者组
        props.put(ConsumerConfig.GROUP_ID_CONFIG, "b1");

        //2.创建consumer实例对象
        KafkaConsumer<String,String> kafkaConsumer=new KafkaConsumer<String,String>(props);

        //3.通过assign方法订阅主题
        //3.1构建相应的分区集合
        TopicPartition tpc_1_0=new TopicPartition("tpc_1",0);
        TopicPartition tpc_1_1=new TopicPartition("tpc_1",1);

        //3.2通过assign实现订阅
        kafkaConsumer.assign(Arrays.asList(tpc_1_0,tpc_1_1));

        //4.拉取消息
        while(true){
            ConsumerRecords<String, String> records = kafkaConsumer.poll(Duration.ofMillis(Long.MAX_VALUE));
            // 这样遍历的话，开发人员无法预知吓一跳遍历到record是哪个主题，哪个分区
            //            for (ConsumerRecord<String, String> record : records) {
            List<ConsumerRecord<String,String>> records1=records.records(tpc_1_0);
            for (ConsumerRecord<String,String> rec:records1){
                System.out.println(rec.key()+","
                        +rec.value()+","
                        +rec.topic()+","
                        +rec.partition()+","
                        +rec.offset());
                System.out.println("---------------------------------------------------");
            }

            List<ConsumerRecord<String,String>> records2=records.records(tpc_1_0);
            for (ConsumerRecord<String,String> rec:records2){
                System.out.println(rec.key()+","
                        +rec.value()+","
                        +rec.topic()+","
                        +rec.partition()+","
                        +rec.offset());
                System.out.println("---------------------------------------------------");
            }
        }

    }

}