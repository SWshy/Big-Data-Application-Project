package Stream_Kafka;

import org.apache.kafka.clients.admin.AdminClient;
import org.apache.kafka.clients.admin.AdminClientConfig;
import org.apache.kafka.clients.admin.NewTopic;

import java.util.Collections;
import java.util.Properties;

public class CreateTopicExample {
    public static void main(String[] args) {
        // 设置Kafka服务器地址
        Properties properties = new Properties();
        properties.put(AdminClientConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092");

        // 创建AdminClient
        AdminClient adminClient = AdminClient.create(properties);

        // 创建主题配置
        NewTopic newTopic = new NewTopic("douban", 3, (short) 1);

        // 创建主题
        adminClient.createTopics(Collections.singleton(newTopic));
        System.out.println("创建主题douban成功!");
        // 关闭AdminClient
        adminClient.close();
    }
}
