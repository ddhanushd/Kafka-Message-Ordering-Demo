package com.demo.consumer;

import com.demo.event.OrderEvent;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.kafka.support.KafkaHeaders;
import org.springframework.messaging.handler.annotation.Header;
import org.springframework.stereotype.Service;

@Service
public class DeadLetterConsumer {
    @KafkaListener(topics = "orders-dlt")
    public void consume(OrderEvent event,
                        @Header(KafkaHeaders.OFFSET) long offset,
                        @Header(KafkaHeaders.RECEIVED_PARTITION) int partition) {

        System.out.printf(
                "DLT -> Partition=%d Offset=%d Order=%s%n",
                partition,
                offset,
                event.getOrderId()
        );
    }
}
