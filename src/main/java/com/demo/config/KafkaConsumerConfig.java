package com.demo.config;

import com.demo.event.OrderEvent;
import org.apache.kafka.clients.consumer.Consumer;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.kafka.config.ConcurrentKafkaListenerContainerFactory;
import org.springframework.kafka.core.ConsumerFactory;
import org.springframework.kafka.listener.ContainerProperties;
import org.springframework.kafka.listener.DefaultErrorHandler;
import org.springframework.kafka.listener.RecordInterceptor;

@Configuration
public class KafkaConsumerConfig {

    @Bean
    public ConcurrentKafkaListenerContainerFactory<String, OrderEvent> batchFactory(
            ConsumerFactory<String, OrderEvent> consumerFactory, DefaultErrorHandler errorHandler) {

        ConcurrentKafkaListenerContainerFactory<String, OrderEvent> factory =
                new ConcurrentKafkaListenerContainerFactory<>();

        factory.setConsumerFactory(consumerFactory);

        // Enable Batch Mode
        factory.setBatchListener(true);

        // 2 consumer threads
        factory.setConcurrency(2);

        // Enable Manual Acknowledgment
        factory.getContainerProperties()
                .setAckMode(ContainerProperties.AckMode.MANUAL);

        // Attach the custom error handler
        factory.setCommonErrorHandler(errorHandler);

        return factory;
    }

}
