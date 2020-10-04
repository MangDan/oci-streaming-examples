package com.example.app;

import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer011;
import org.apache.flink.api.common.serialization.SimpleStringSchema;
import java.io.FileNotFoundException;
import java.io.FileReader;
import java.util.Properties;

@SuppressWarnings("all")
public class FlinkTestConsumer {

    private static final String TOPIC = "flink-topic";
    private static final String FILE_PATH = "src/main/resources/consumer.config";

    public static void main(String... args) {
        try {
            //Load properties from config file
            Properties properties = new Properties();
            properties.load(new FileReader(FILE_PATH));
            
            final StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
            DataStream<String> stream = env.addSource(new FlinkKafkaConsumer011<String>(TOPIC, new SimpleStringSchema(), properties));
            stream.print();
            env.execute("Testing flink consumer");

        } catch(FileNotFoundException e){
            System.out.println("FileNoteFoundException: " + e);
        } catch (Exception e){
            System.out.println("Failed with exception " + e);
        }
    }
}