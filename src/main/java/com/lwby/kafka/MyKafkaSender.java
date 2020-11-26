package com.lwby.kafka;


import org.apache.kafka.clients.producer.*;
import org.apache.kafka.common.serialization.StringSerializer;

import java.util.Properties;
import java.util.concurrent.ExecutionException;

public class MyKafkaSender {

    public static KafkaProducer<String, String> kafkaProducer = null;


    public static KafkaProducer<String, String> createKafkaProducer() {
        Properties properties = new Properties();
        properties.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, "bi-kafka01:6667,bi-kafka02:6667,bi-kafka03:6667");
        properties.put(ProducerConfig.CLIENT_ID_CONFIG, "canal");
        properties.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        properties.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        KafkaProducer<String, String> producer = null;
        try {
            producer = new KafkaProducer<String, String>(properties);

        } catch (Exception e) {
            e.printStackTrace();
        }
        return producer;
    }

    public static void send(String topic, String msg){
        if (kafkaProducer == null) {
            kafkaProducer = createKafkaProducer();
        }
        kafkaProducer.send(new ProducerRecord<String, String>(topic, msg));



    }
//    class DemoCallBack implements Callback {
//
//        private final long startTime;
//        private final int key;
//        private final String message;
//
//        public DemoCallBack(long startTime, int key, String message) {
//            this.startTime = startTime;
//            this.key = key;
//            this.message = message;
//        }
//
//        /**
//         * A callback method the user can implement to provide asynchronous handling of request completion. This method will
//         * be called when the record sent to the server has been acknowledged. When exception is not null in the callback,
//         * metadata will contain the special -1 value for all fields except for topicPartition, which will be valid.
//         *
//         * @param metadata  The metadata for the record that was sent (i.e. the partition and offset). Null if an error
//         *                  occurred.
//         * @param exception The exception thrown during processing of this record. Null if no error occurred.
//         */
//        public void onCompletion(RecordMetadata metadata, Exception exception) {
//            long elapsedTime = System.currentTimeMillis() - startTime;
//            if (metadata != null) {
//                System.out.println(
//                        "message(" + key + ", " + message + ") sent to partition(" + metadata.partition() +
//                                "), " +
//                                "offset(" + metadata.offset() + ") in " + elapsedTime + " ms");
//            } else {
//                exception.printStackTrace();
//            }
//        }
//    }
}

