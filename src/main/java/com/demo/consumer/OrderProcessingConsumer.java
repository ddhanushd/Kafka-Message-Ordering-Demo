package com.demo.consumer;

import com.demo.event.OrderEvent;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.stereotype.Component;

@Component
public class OrderProcessingConsumer {
    private final KafkaTemplate<String, OrderEvent> kafkaTemplate;

    public OrderProcessingConsumer(
            KafkaTemplate<String, OrderEvent> kafkaTemplate) {
        this.kafkaTemplate = kafkaTemplate;
    }

    @KafkaListener(
            topics = "orders",
            groupId = "eos-order-group",
            containerFactory = "eosKafkaListenerContainerFactory"
    )
    public void processOrder(OrderEvent event) {

        System.out.println("EOS input: " + event.getOrderId());

        kafkaTemplate.send(
                "processed-orders",
                event.getOrderId(),
                event
        );

//        // Add the failure test here
//        if ("ORD-EOS-FAIL".equals(event.getOrderId())) {
//            throw new RuntimeException("Simulated EOS processing failure");
//        }

        System.out.println("EOS output sent: " + event.getOrderId());
    }
}
