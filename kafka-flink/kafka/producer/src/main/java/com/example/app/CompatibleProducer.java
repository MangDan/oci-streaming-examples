package com.example.app;

import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.StringSerializer;

import java.util.Properties;
import java.util.UUID;

@SuppressWarnings("all")
public class CompatibleProducer {

    public void produce() {
        String authToken = "NX-N)x0Iz2_SC:Yc7zp(";
        String tenancyName = "cltonetiger2";
        String username = "oracleidentitycloudservice/donghu.kim@oracle.com";
        String streamPoolId = "ocid1.streampool.oc1.ap-seoul-1.amaaaaaaamrxswyaae6jgjd6czstcn6whxfii24s4zdujslfzs3pn7duihlq";
        String topicName = "flink-topic";

        Properties properties = new Properties();
        properties.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, "cell-1.streaming.ap-seoul-1.oci.oraclecloud.com:9092");
        properties.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        properties.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        properties.put(ProducerConfig.TRANSACTION_TIMEOUT_CONFIG, 5000);
        properties.put(ProducerConfig.DELIVERY_TIMEOUT_MS_CONFIG, 5000);
        properties.put(ProducerConfig.REQUEST_TIMEOUT_MS_CONFIG, 5000);
        properties.put("sasl.mechanism", "PLAIN");
        properties.put("security.protocol", "SASL_SSL");
        properties.put("sasl.jaas.config",
                        "org.apache.kafka.common.security.plain.PlainLoginModule required username=\""
                        + tenancyName + "/"
                        + username + "/"
                        + streamPoolId + "\" "
                        + "password=\""
                        + authToken + "\";"
        );
        properties.put("retries", 5); // retries on transient errors and load balancing disconnection
        properties.put("max.request.size", 1024 * 1024); // limit request size to 1MB

        KafkaProducer producer = new KafkaProducer(properties);
        
        for (int i = 0; i < 5; i++) {
            ProducerRecord record = new ProducerRecord(topicName, UUID.randomUUID().toString(), "Test record #" + i);
            
            producer.send(record, (md, ex) -> {
                if( ex != null ) {
                    ex.printStackTrace();
                }
                else {
                    System.out.println(
                            "Sent msg to "
                                    + md.partition()
                                    + " with offset "
                                    + md.offset()
                                    + " at "
                                    + md.timestamp()
                    );
                }
            });
        }
        producer.flush();
        producer.close();
        System.out.println("produced 5 messages");
    }

}