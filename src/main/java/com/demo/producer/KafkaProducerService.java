package com.demo.producer;

import com.demo.event.OrderEvent;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.kafka.core.ProducerFactory;
import org.springframework.stereotype.Service;

import java.time.LocalDateTime;
import java.util.UUID;

@Service
public class KafkaProducerService {

    private final KafkaTemplate<String, OrderEvent> kafkaTemplate;
    private final ProducerFactory<String, OrderEvent> producerFactory;

    public KafkaProducerService(
            KafkaTemplate<String, OrderEvent> kafkaTemplate,
            ProducerFactory<String, OrderEvent> producerFactory) {

        this.kafkaTemplate = kafkaTemplate;
        this.producerFactory = producerFactory;
    }

    public void printProducerMetrics() {

        producerFactory.createProducer()
                .metrics()
                .forEach((metricName, metric) -> {

                    String name = metricName.name();

                    if (name.equals("records-per-request-avg")
                            || name.equals("batch-size-avg")
                            || name.equals("batch-size-max")) {

                        System.out.printf(
                                "%s = %s%n",
                                name,
                                metric.metricValue()
                        );
                    }
                });
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

        kafkaTemplate.send(
                "orders",
                event.getOrderId(),
                event
        );

        System.out.println("=== WITH KEY ===");
    }
}