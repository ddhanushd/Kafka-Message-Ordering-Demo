package com.demo.producer;

import com.demo.event.OrderEvent;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.stereotype.Service;

import java.time.LocalDateTime;
import java.util.UUID;

@Service
public class KafkaProducerService {
    private final KafkaTemplate<String, OrderEvent> kafkaTemplate;

    public KafkaProducerService(KafkaTemplate<String, OrderEvent> kafkaTemplate) {
        this.kafkaTemplate = kafkaTemplate;
    }

    // Without Key
    public void sendWithoutKey(OrderEvent event) {

        event.setEventId(UUID.randomUUID());
        event.setEventTime(LocalDateTime.now().toString());

        kafkaTemplate.send("orders", event);
        System.out.println("=== WITHOUT KEY ===");
    }

    // With Key
    public void sendWithKey(OrderEvent event) {

        event.setEventId(UUID.randomUUID());
        event.setEventTime(LocalDateTime.now().toString());

        kafkaTemplate.send("orders", event.getOrderId(), event);
        System.out.println("=== WITH KEY ===");
    }
}
