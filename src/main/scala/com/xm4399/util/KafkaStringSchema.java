package com.xm4399.util;

import org.apache.flink.api.common.typeinfo.TypeHint;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.streaming.connectors.kafka.KafkaDeserializationSchema;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.common.record.TimestampType;

import java.nio.charset.StandardCharsets;

public class KafkaStringSchema implements KafkaDeserializationSchema<org.apache.kafka.clients.consumer.ConsumerRecord<String, String>> {



    //private org.apache.kafka.clients.consumer.ConsumerRecord<String,String> ConsumerRecord;

    //private Object String;

    @Override
    public boolean isEndOfStream(org.apache.kafka.clients.consumer.ConsumerRecord<String, String> stringStringConsumerRecord) {
        return false;
    }

    @Override
    public org.apache.kafka.clients.consumer.ConsumerRecord<String, String> deserialize(org.apache.kafka.clients.consumer.ConsumerRecord<byte[], byte[]> consumerRecord) throws Exception {
        String key = null;
        if(consumerRecord.key() != null){
           // key = new String(consumerRecord.key(),"UTF-8");
            key = new String(consumerRecord.key(), StandardCharsets.UTF_8);
        }

        return new ConsumerRecord<String,String>(consumerRecord.topic(),
                consumerRecord.partition(),
                consumerRecord.offset(),
                consumerRecord.timestamp(),
                TimestampType.CREATE_TIME,
                consumerRecord.checksum(),
                consumerRecord.serializedKeySize(),
                consumerRecord.serializedValueSize(),
                key,
                //new String(consumerRecord.value(),"UTF-8")
                new String(consumerRecord.value(), StandardCharsets.UTF_8)
        );

    }

    @Override
    public TypeInformation<org.apache.kafka.clients.consumer.ConsumerRecord<String, String>> getProducedType() {
        /*ArrayList<String>.getclass;
        String.getClass();
        String.getClass();
        ConsumerRecord<String, String> ff=new ConsumerRecord<String,String>();
        ff.getClass();
        return TypeInformation.of();*/

        return TypeInformation.of(new TypeHint<org.apache.kafka.clients.consumer.ConsumerRecord<String,String>>(){});


    }
    //override def getProducedType: TypeInformation[ConsumerRecord[String, String]] = TypeInformation.of(classOf[ConsumerRecord[String, String]])

}
