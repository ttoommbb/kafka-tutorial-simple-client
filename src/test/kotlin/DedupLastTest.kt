import com.ippontech.kafkatutorials.simpleclient.MexDedupProcessorSupplier
import org.apache.kafka.clients.consumer.ConsumerConfig
import org.apache.kafka.common.serialization.Serdes
import org.apache.kafka.streams.KafkaStreams
import org.apache.kafka.streams.StreamsBuilder
import org.apache.kafka.streams.StreamsConfig
import org.junit.Test
import java.util.*
import java.util.concurrent.CountDownLatch


class DedupLastTest {

    @Test
    fun test() {
        val builder = StreamsBuilder()
        val mexDedupSupplier = MexDedupProcessorSupplier<String>(STATUS_STORE_NAME)
//        builder.addStateStore(mexDedupSupplier.storeBuilder)
//        builder
//                .stream<String, ByteBuffer>(TOPIC_ORIGINAL)
//                .process(mexDedupSupplier)

        val topology = builder.build()


        topology.addSource("Source", TOPIC_ORIGINAL)
                .addProcessor("Process", mexDedupSupplier, "Source")
                .addStateStore(mexDedupSupplier.storeBuilder, "Process")
                .addSink("Sink", "sink-topic", "Process")
//        topology.addStateStore(mexDedupProcessorSupplier.storeBuilder, TOPIC_ORIGINAL)

        val streams = KafkaStreams(topology, props)


        //---------start streams and prevent stop on main----------
        val latch = CountDownLatch(1)

        // ... same as Pipe.java above
        Runtime.getRuntime().addShutdownHook(object : Thread("streams-shutdown-hook") {
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

    companion object {
        const val STATUS_STORE_NAME = "lastHandled"
        const val TOPIC_ORIGINAL = "plugintest"
        private const val HOST = "192.168.1.40:9092"


        val props = Properties().apply {
            this[StreamsConfig.APPLICATION_ID_CONFIG] = TOPIC_ORIGINAL
            this[StreamsConfig.BOOTSTRAP_SERVERS_CONFIG] = HOST
            this[StreamsConfig.DEFAULT_KEY_SERDE_CLASS_CONFIG] = Serdes.String().javaClass
            this[StreamsConfig.DEFAULT_VALUE_SERDE_CLASS_CONFIG] = Serdes.ByteBuffer().javaClass
            this[ConsumerConfig.AUTO_OFFSET_RESET_CONFIG] = "earliest"
        }
    }
}