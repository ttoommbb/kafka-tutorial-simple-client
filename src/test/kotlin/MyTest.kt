import org.apache.commons.codec.binary.Hex
import org.apache.kafka.clients.consumer.ConsumerConfig
import org.apache.kafka.common.cache.LRUCache
import org.apache.kafka.common.serialization.Serdes
import org.apache.kafka.streams.KafkaStreams
import org.apache.kafka.streams.StreamsBuilder
import org.apache.kafka.streams.StreamsConfig
import org.junit.Test
import java.nio.ByteBuffer
import java.nio.ByteOrder
import java.util.*
import java.util.concurrent.CountDownLatch


class MyTest {

    @Test
    fun test() {
        //duplicate it
//        DupThread().start()

//➜  bin/kafka-console-consumer.sh --bootstrap-server 192.168.1.40:9092 --topic plugintest --from-beginning
//➜  bin/kafka-console-consumer.sh --bootstrap-server 192.168.1.40:9092 --topic streams-out --from-beginning

        val actionCache = LRUCache<Long, Boolean>(1000)
        val blockCache = LRUCache<Int, Boolean>(1000)

        val builder = StreamsBuilder()
        builder
                .stream<String, ByteBuffer>(TOPIC_ORIGINAL)
                .peek { key, value ->
                                        println("in::: key:$key, ${String(Hex.encodeHex(value))}")
                }
                .filter { _, value ->
                    value.order(ByteOrder.LITTLE_ENDIAN)
                    when (value.get()) {
                        ACTION_MESSAGE_INDICATOR -> println("action::: ${String(Hex.encodeHex(value))}")
                        BLOCK_MESSAGE_INDICATOR -> {
//                            println("block::: ${String(Hex.encodeHex(value))}")
                            val numberByte = value.getInt()
                            val exist = blockCache.get(numberByte)
                            if (exist == true) {
                                return@filter false
                            }
                            blockCache.put(numberByte, true)
                            return@filter true
                        }
                        else -> println("unknown::: ${String(Hex.encodeHex(value))}")
                    }
                    true
                }
                .peek { key, value ->
                    println("out::: key:$key, ${String(Hex.encodeHex(value))}")
                }
                .to("streams-out")


        val topology = builder.build()

        val streams = KafkaStreams(topology, props)
        streams.start()
        val latch = CountDownLatch(1)

        // ... same as Pipe.java above
        Runtime.getRuntime().addShutdownHook(object : Thread("streams-wordcount-shutdown-hook") {
            override fun run() {
                streams.close()
                latch.countDown()
            }
        })

        try {
            latch.await()
        } catch (e: Throwable) {
            System.exit(1)
        }

        System.exit(0)
    }

    class LoadOutThread : Thread() {

        override fun run() {
            StreamsBuilder().stream<String, String>(TOPIC_STREAM_OUT)
                    .peek { key, value ->
                        println("out::: key:$key, value:$value")
                    }
        }

    }

    class DupThread : Thread() {

        override fun run() {
//            val file = File("${Thread.currentThread().name}.log")
//            val out = file.outputStream()
//            val objOut = ObjectOutputStream(out)
            val builder = StreamsBuilder()
            builder.stream<String, String>(TOPIC_ORIGINAL)
//                    .peek { _, value ->
//                        objOut.writeObject(value)
//                        objOut.flush()
////                        println("${Thread.currentThread().name}::: key:$key, value:$value")
//                    }
                    .to(TOPIC_STREAM_DUP)

            val topology = builder.build()

//            props[ConsumerConfig.GROUP_ID_CONFIG] = Thread.currentThread().name
            KafkaStreams(topology, props).start()
        }
    }

    companion object {
        val TOPIC_ORIGINAL = "plugintest"
        val TOPIC_STREAM_DUP = "streams-dup"
        val TOPIC_STREAM_OUT = "streams-out"
        val HOST = "192.168.1.40:9092"
        val ACTION_MESSAGE_INDICATOR: Byte = 1
        val BLOCK_MESSAGE_INDICATOR: Byte = 2

        val props = Properties().apply {
            this[StreamsConfig.APPLICATION_ID_CONFIG] = TOPIC_ORIGINAL
            this[StreamsConfig.BOOTSTRAP_SERVERS_CONFIG] = HOST
            this[StreamsConfig.DEFAULT_KEY_SERDE_CLASS_CONFIG] = Serdes.String().javaClass
            this[StreamsConfig.DEFAULT_VALUE_SERDE_CLASS_CONFIG] = Serdes.ByteBuffer().javaClass
            this[ConsumerConfig.AUTO_OFFSET_RESET_CONFIG] = "earliest"
        }
    }
}