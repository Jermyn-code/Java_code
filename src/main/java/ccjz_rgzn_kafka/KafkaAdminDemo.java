package ccjz_rgzn_kafka;

import org.apache.kafka.clients.admin.*;
import org.apache.kafka.common.KafkaFuture;
import org.apache.kafka.common.TopicPartition;

import java.util.Arrays;
import java.util.Map;
import java.util.Properties;
import java.util.Set;
import java.util.concurrent.ExecutionException;

/**
 * Package: ccjz_rgzn_kafka
 * Description：
 * Author: Jermyn
 * Date: Created in 2022/4/27 0027 10:52
 * Version: 0.0.1
 */
public class KafkaAdminDemo {
    private static final String SERVERS = "master:9092,slave1:9092,slave2:9092";

    public static void main(String[] args) throws ExecutionException, InterruptedException {
        Properties props = new Properties();
        props.setProperty(AdminClientConfig.BOOTSTRAP_SERVERS_CONFIG, SERVERS);
        /*1.构造一个客户端的对象*/
        AdminClient adminClient = KafkaAdminClient.create(props);
        /*2.列出集群中的主题信息*/
        ListTopicsResult listTopicsResult = adminClient.listTopics();
        KafkaFuture<Set<String>> names = listTopicsResult.names();
        Set<String> topicNames = names.get();
        System.out.println(topicNames);

        //3.查看一个topic的具体信息
        DescribeTopicsResult tpc_1 = adminClient.describeTopics(Arrays.asList("tpc_1", "tpc_2", "topic_1", "tpc_4"));
        KafkaFuture<Map<String, TopicDescription>> future = tpc_1.all();
        Map<String, TopicDescription> stringTopicDescriptionMap = future.get();//get()会阻塞直到拿到返回值
        Set<Map.Entry<String, TopicDescription>> entries = stringTopicDescriptionMap.entrySet();
        for (Map.Entry<String, TopicDescription> entry : entries) {
            System.out.println(entry.getKey());
            TopicDescription desc = entry.getValue();
            System.out.println(desc.name() + "," + desc.partitions());
            System.out.println("----------分割线---------------");



        }
    }
}

