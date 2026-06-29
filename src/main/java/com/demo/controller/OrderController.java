package com.demo.controller;

import com.demo.event.OrderEvent;
import com.demo.producer.KafkaProducerService;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.*;

@RestController
@RequestMapping("/orders")
public class OrderController {

    private final KafkaProducerService producerService;

    public OrderController(KafkaProducerService producerService) {
        this.producerService = producerService;
    }

    @PostMapping("/without-key")
    public ResponseEntity<String> sendWithoutKey(@RequestBody OrderEvent event) {

        producerService.sendWithoutKey(event);

        return ResponseEntity.ok("Event sent without key");
    }

    @PostMapping("/with-key")
    public ResponseEntity<String> sendWithKey(@RequestBody OrderEvent event) {

        producerService.sendWithKey(event);

        return ResponseEntity.ok("Event sent with key");
    }


//    @PostMapping("/generate")
//    public ResponseEntity<String> generateMessages(
//            @RequestParam(defaultValue = "20") int count) throws InterruptedException {
//
//        for (int i = 1; i <= count; i++) {
//            producerService.send("Order-" + i);
//            Thread.sleep(500);
//        }
//
//        return ResponseEntity.ok(count + " messages sent.");
//    }
//
//    @PostMapping("/keyed")
//    public ResponseEntity<String> sendWithKey(
//            @RequestParam String key,
//            @RequestBody String message) {
//
//        producerService.send(key, message);
//
//        return ResponseEntity.ok("Sent with key: " + key);
//    }
}
