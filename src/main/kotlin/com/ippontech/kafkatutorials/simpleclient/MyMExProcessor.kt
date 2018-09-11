package com.ippontech.kafkatutorials.simpleclient

import org.apache.kafka.clients.consumer.Consumer
import org.apache.kafka.clients.consumer.KafkaConsumer
import org.apache.kafka.common.serialization.StringDeserializer
import org.apache.log4j.LogManager
import java.time.Duration
import java.util.*



fun main(args: Array<String>) {
    MyMExProcessor("192.168.1.40:9092").process()
}

class MyMExProcessor(brokers: String) {

    private val logger = LogManager.getLogger(javaClass)
    private val consumer = createConsumer(brokers)

    private fun createConsumer(brokers: String): Consumer<String, String> {
        val props = Properties()
        props["bootstrap.servers"] = brokers
        props["group.id"] = "person-processor"
        props["key.deserializer"] = StringDeserializer::class.java
        props["value.deserializer"] = StringDeserializer::class.java
//        props
        return KafkaConsumer<String, String>(props)
    }



    fun process() {
        consumer.subscribe(listOf("plugintest"))

        logger.info("Consuming and processing data")

        while (true) {
            val records = consumer.poll(Duration.ofSeconds(1))
            logger.info("Received ${records.count()} records")

            records.iterator().forEach {
                val personJson = it.value()
                logger.debug("JSON data: $personJson")

            }
        }
    }
}
