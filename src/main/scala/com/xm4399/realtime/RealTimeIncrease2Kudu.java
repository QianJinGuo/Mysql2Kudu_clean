package com.xm4399.realtime;

import com.xm4399.util.KafkaStringSchema;
import com.xm4399.util.SubTableKuduSink;
import org.apache.flink.streaming.api.datastream.DataStreamSink;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer;
import org.apache.kafka.clients.consumer.ConsumerRecord;

import java.util.Properties;

public class RealTimeIncrease2Kudu {

    public  void realTimeIncrease (String isSubTable) {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        //  从kafka中读取数据
        // 创建kafka相关的配置
        Properties properties = new Properties();
        //properties.setProperty("bootstrap.servers", "localhost:9092");
        properties.setProperty("bootstrap.servers", "10.0.0.194:9092,10.0.0.195:9092,10.0.0.199:9092");
        properties.setProperty("group.id", "aa");
        properties.setProperty("key.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");
        properties.setProperty("value.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");
        properties.setProperty("auto.offset.reset", "latest");
        Properties props = new Properties();

        DataStreamSink<ConsumerRecord<String,String>> stream = env
                .addSource(new FlinkKafkaConsumer<ConsumerRecord<String,String>>("chenzhikun_test", new KafkaStringSchema(), properties))
                //.addSource(new FlinkKafkaConsumer<ConsumerRecord<String,String>>("qianduan_test", new KafkaStringSchema(), properties))
                .addSink(new SubTableKuduSink(isSubTable));
        try {
            env.execute();
        } catch (Exception exception) {
            exception.printStackTrace();
        }
    }

}
