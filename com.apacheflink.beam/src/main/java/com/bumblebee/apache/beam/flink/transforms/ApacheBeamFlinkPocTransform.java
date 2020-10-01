package com.bumblebee.apache.beam.flink.transforms;

import org.apache.beam.sdk.io.kafka.KafkaRecord;
import org.apache.beam.sdk.transforms.DoFn;
import org.apache.beam.sdk.values.KV;

public class ApacheBeamFlinkPocTransform extends DoFn<KafkaRecord<String,String>,KV<String,String>> {
    //use this
    @ProcessElement
    public void processElement(ProcessContext c)
    {
        //take input element
        KafkaRecord<String,String> kafkaRecord =  c.element();
        //print
        KV<String,String> kv = kafkaRecord.getKV();
        System.out.println("Key:"+kv.getKey());
        System.out.println("Value:"+kv.getValue());
        c.output(kv);
    }
}
