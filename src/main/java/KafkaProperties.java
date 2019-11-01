import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.producer.ProducerConfig;

import java.util.Properties;

public class KafkaProperties {
    private static final String BOOTSTRAP_SERVERS = "redhat4:9092,redhat5:9092";
    private static final String KEY_SERIALIZER_CLASS_CONFIG = "org.apache.kafka.common.serialization.StringSerializer";
    private static final String VALUE_SERIALIZER_CLASS_CONFIG = "org.apache.kafka.common.serialization.StringSerializer";
    private static final String PROTOCOL = "SASL_PLAINTEXT";
    private static final String SERVICE_NAME = "kafka";
    private static final String CONSUMER_GROUP = "DemoConsumer";
    private static final String KEY_DESERIALIZER_CLASS_CONFIG = "org.apache.kafka.common.serialization.StringDeserializer";
    private static final String VALUE_DESERIALIZER_CLASS_CONFIG = "org.apache.kafka.common.serialization.StringDeserializer";

    public static void loadConf()
    {
        System.setProperty("java.security.krb5.conf","myConf/krb5.conf");
        System.setProperty("java.security.auth.login.config","myConf/jaas.conf");
        System.setProperty("javax.security.auth.useSubjectCredsOnly","false");
    }
    public static Properties getProducerPro()
    {
        Properties props = new Properties();
        props.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG,BOOTSTRAP_SERVERS);
        props.put(ProducerConfig.ACKS_CONFIG,"all");
        props.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG,KEY_SERIALIZER_CLASS_CONFIG);
        props.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG,VALUE_SERIALIZER_CLASS_CONFIG);
        props.put("security.protocol",PROTOCOL);
        props.put("sasl.kerberos.service.name",SERVICE_NAME);
        return props;
    }
    public static Properties getConsumerPro()
    {
        Properties props = new Properties();
        props.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG,BOOTSTRAP_SERVERS);
        props.put(ConsumerConfig.GROUP_ID_CONFIG,CONSUMER_GROUP);
        props.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG,KEY_DESERIALIZER_CLASS_CONFIG);
        props.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG,VALUE_DESERIALIZER_CLASS_CONFIG);
        props.put(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG,"true");
        props.put(ConsumerConfig.AUTO_COMMIT_INTERVAL_MS_CONFIG,"1000");
        props.put("security.protocol",PROTOCOL);
        props.put("sasl.kerberos.service.name",SERVICE_NAME);
        return props;
    }
}
