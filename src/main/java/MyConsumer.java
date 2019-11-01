import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;

import java.util.Arrays;
import java.util.Properties;

public class MyConsumer {
    public static String TOPIC_NAME="kafkatest2";
    public static void main(String[] args) {
        KafkaProperties.loadConf();
        Properties props = KafkaProperties.getConsumerPro();
        KafkaConsumer<String,String> consumer = new KafkaConsumer<String, String>(props);


        consumer.subscribe(Arrays.asList(TOPIC_NAME));
        //consumer.assign(Arrays.asList(partition0));

        ConsumerRecords<String,String> records = null;
        while (true)
        {

                System.out.println();
                records = consumer.poll(1000);
                for (ConsumerRecord<String,String> record:records)
                {
                    System.out.println("Receivedmessage:("+record.key()+","+record.value()+") at offset"+record.offset());

                }

        }
    }
}
