import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.ProducerRecord;

import java.util.Properties;

public class MyProducer {
    public static String TOPIC_NAME="kafkatest2";

    public static void main(String[] args) {
        KafkaProperties.loadConf();
        Properties props = KafkaProperties.getProducerPro();
        Producer<String,String> producer = new KafkaProducer<String, String>(props);
        for (int i = 0;i<100;i++)
        {
            String key = "key"+i;
            String message = "Message"+i;
            ProducerRecord record = new ProducerRecord<String,String> (TOPIC_NAME,key,message);
            producer.send(record);
            System.out.println(key+"====="+message);
        }
        producer.close();

    }
}
