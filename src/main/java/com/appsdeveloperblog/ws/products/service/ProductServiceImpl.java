package com.appsdeveloperblog.ws.products.service;

import com.appsdeveloperblog.ws.core.ProductCreatedEvent;
import com.appsdeveloperblog.ws.products.rest.CreateProductRestModel;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.kafka.support.SendResult;
import org.springframework.stereotype.Service;

import java.util.UUID;
import java.util.concurrent.CompletableFuture;

@Service
public class ProductServiceImpl implements ProductService{


    KafkaTemplate<String, ProductCreatedEvent> kafkaTemplate;
    private final Logger LOGGER = LoggerFactory.getLogger(this.getClass());


    public ProductServiceImpl(KafkaTemplate<String, ProductCreatedEvent> kafkaTemplate) {
        this.kafkaTemplate = kafkaTemplate;
    }

    @Override
    public String createProduct(CreateProductRestModel product) throws Exception {
        String productId = UUID.randomUUID().toString();

        //todo Persist Product into db

        ProductCreatedEvent productCreatedEvent = new ProductCreatedEvent(productId,
                product.getTitle(), product.getPrice(), product.getQuantity());

        LOGGER.info("Before publishing a ProductCreatedEvent");

        ProducerRecord<String, ProductCreatedEvent> record = new ProducerRecord<>("product-created-events-topic", productId, productCreatedEvent);
        record.headers().add("messageId", UUID.randomUUID().toString().getBytes());
        SendResult<String, ProductCreatedEvent> result = kafkaTemplate.send(record).get();

//        CompletableFuture<SendResult<String, ProductCreatedEvent>> future = kafkaTemplate.send("product-created-events-topic", productId, productCreatedEvent);
//        future.whenComplete((result, exception) -> {
//            if(exception != null) {
//                LOGGER.error("Failed to send message:" + exception.getMessage());
//            } else {
//                LOGGER.info("Message sent successfully:" + result.getRecordMetadata());
//            }
//        });

        // One option to make KAFKA synchronous, wait future for the result
        //future.join();
        LOGGER.info("Partitions:" + result.getRecordMetadata().partition());
        LOGGER.info("Topic:" + result.getRecordMetadata().topic());
        LOGGER.info("Offset:" + result.getRecordMetadata().offset());

        LOGGER.info("***** Returning product id");
        return productId;
    }
}
