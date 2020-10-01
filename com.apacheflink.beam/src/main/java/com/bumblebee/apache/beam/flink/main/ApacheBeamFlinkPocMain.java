package com.bumblebee.apache.beam.flink.main;


import com.bumblebee.apache.beam.flink.transforms.ApacheBeamFlinkPocTransform;
import org.apache.beam.sdk.Pipeline;
import org.apache.beam.sdk.io.kafka.KafkaIO;
import org.apache.beam.sdk.options.PipelineOptions;
import org.apache.beam.sdk.options.PipelineOptionsFactory;
import org.apache.beam.sdk.transforms.ParDo;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.apache.kafka.common.serialization.StringSerializer;

public class ApacheBeamFlinkPocMain {
    public static void main(String args[])
    {

        PipelineOptions options = PipelineOptionsFactory.fromArgs(args).withValidation().create();
        Pipeline pipeline = Pipeline.create(options);
        pipeline.apply(KafkaIO.<String, String>read()
                .withBootstrapServers("192.168.50.211:19092")
                .withTopic("testData")
                .withKeyDeserializer(StringDeserializer.class)
                .withValueDeserializer(StringDeserializer.class))
                .apply(ParDo.of(new ApacheBeamFlinkPocTransform()))
                .apply(KafkaIO.<String,String>write().
                        withBootstrapServers("192.168.50.211:19092")
                .withKeySerializer(StringSerializer.class)
                .withValueSerializer(StringSerializer.class)
                .withTopic("testData2"));

        pipeline.run().waitUntilFinish();
    }
}
