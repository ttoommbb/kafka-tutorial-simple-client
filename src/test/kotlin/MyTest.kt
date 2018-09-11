import org.apache.kafka.common.serialization.Serdes
import org.apache.kafka.streams.KafkaStreams
import org.apache.kafka.streams.StreamsBuilder
import org.apache.kafka.streams.StreamsConfig
import org.apache.kafka.streams.kstream.Produced
import org.junit.Test
import java.io.File
import java.io.ObjectOutputStream
import java.util.*
import java.util.concurrent.CountDownLatch


class MyTest {

    @Test
    fun test() {
        val builder = StreamsBuilder()

        //duplicate it
        DupThread().start()
        DupThread().start()
        DupThread().start()
        LoadOutThread().start()

//➜  bin/kafka-console-consumer.sh --bootstrap-server 192.168.1.40:9092 --topic plugintest --from-beginning
//➜  bin/kafka-console-consumer.sh --bootstrap-server 192.168.1.40:9092 --topic streams-out --from-beginning

        builder
                .stream<String, String>(TOPIC_STREAM_DUP)
                .to("streams-out", Produced.with(Serdes.String(), Serdes.String()))

        val topology = builder.build()
        val streams = KafkaStreams(topology, props)
        val latch = CountDownLatch(1)

        // ... same as Pipe.java above
        Runtime.getRuntime().addShutdownHook(object : Thread("streams-wordcount-shutdown-hook") {
            override fun run() {
                streams.close()
                latch.countDown()
            }
        })

        try {
            streams.start()
            latch.await()
        } catch (e: Throwable) {
            System.exit(1)
        }

        System.exit(0)
    }

    class LoadOutThread: Thread() {

        override fun run() {
            StreamsBuilder().stream<String, String>(TOPIC_STREAM_OUT)
                    .peek { key, value ->
                        println("key:$key, value:$value")
                    }
        }

    }

    class DupThread : Thread() {

        override fun run() {
            val file = File("${Thread.currentThread().name}.log")
            val out = file.outputStream()
            val objOut = ObjectOutputStream(out)
            val builder = StreamsBuilder()
            builder.stream<String, String>(TOPIC_ORIGINAL)
                    .peek { key, value ->
                        objOut.writeObject(value)
                        objOut.flush()
                        println("${Thread.currentThread().name}::: key:$key, value:$value")
                    }
                    .to(TOPIC_STREAM_DUP)

            val topology = builder.build()
            KafkaStreams(topology, props).start()
        }
    }

    companion object {
        val TOPIC_ORIGINAL = "plugintest"
        val TOPIC_STREAM_DUP = "streams-dup"
        val TOPIC_STREAM_OUT = "streams-out"
        val HOST = "192.168.1.40:9092"
        val props = Properties().apply {
            this[StreamsConfig.APPLICATION_ID_CONFIG] = TOPIC_ORIGINAL
            this[StreamsConfig.BOOTSTRAP_SERVERS_CONFIG] = HOST
            this[StreamsConfig.DEFAULT_KEY_SERDE_CLASS_CONFIG] = Serdes.String().javaClass
            this[StreamsConfig.DEFAULT_VALUE_SERDE_CLASS_CONFIG] = Serdes.String().javaClass
        }
    }
}