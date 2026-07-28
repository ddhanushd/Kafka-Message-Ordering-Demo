package com.demo.consumer;

import com.demo.event.OrderEvent;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.kafka.support.Acknowledgment;
import org.springframework.kafka.support.KafkaHeaders;
import org.springframework.messaging.handler.annotation.Header;
import org.springframework.stereotype.Service;

import java.util.List;

@Service
public class OrderConsumer {

//    @KafkaListener(topics = "orders", groupId = "order-group")
//    public void consume(
//            OrderEvent event,
//            @Header(KafkaHeaders.RECEIVED_PARTITION) int partition,
//            @Header(KafkaHeaders.OFFSET) long offset,
//            @Header(value = KafkaHeaders.RECEIVED_KEY, required = false) String key) {
//
//        System.out.printf(
//                "Partition=%d | Offset=%d | Key=%s | Status=%s | Time=%s%n",
//                partition,
//                offset,
//                key,
//                event.getStatus(),
//                event.getEventTime()
//        );
//    }

    @KafkaListener(
            topics = "orders",
            containerFactory = "batchFactory"
    )
    public void consume(List<ConsumerRecord<String, OrderEvent>> records,
                        Acknowledgment ack) {

        System.out.println("\n========== BATCH ==========");
        System.out.println("Batch Size : " + records.size());

        for (ConsumerRecord<String, OrderEvent> record : records) {

            System.out.printf(
                    "Partition=%d Offset=%d Key=%s Status=%s%n",
                    record.partition(),
                    record.offset(),
                    record.key(),
                    record.value().getStatus()
            );
        }
        //ack.acknowledge();
    }

}
