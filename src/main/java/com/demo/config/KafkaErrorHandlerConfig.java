package com.demo.config;

import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.kafka.listener.DefaultErrorHandler;
import org.springframework.util.backoff.FixedBackOff;

@Configuration
public class KafkaErrorHandlerConfig {
    @Bean
    public DefaultErrorHandler errorHandler() {

        FixedBackOff backOff = new FixedBackOff(2000L, 3);

        return new DefaultErrorHandler(
                (record, ex) -> {
                    System.out.printf(
                            "Recovered -> Partition=%d Offset=%d Key=%s%n",
                            record.partition(),
                            record.offset(),
                            record.key()
                    );
                },
                backOff
        );
    }
}
