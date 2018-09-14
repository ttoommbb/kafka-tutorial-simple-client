package com.ippontech.kafkatutorials.simpleclient

import org.apache.kafka.common.serialization.Serdes
import org.apache.kafka.streams.processor.ProcessorSupplier
import org.apache.kafka.streams.state.Stores
import java.nio.ByteBuffer

class MexDedupProcessorSupplier<K>(private val storeName: String) : ProcessorSupplier<K, ByteBuffer> {
    val storeBuilder = Stores.keyValueStoreBuilder(
            Stores.persistentKeyValueStore(storeName),
            Serdes.Short(),
            Serdes.Long()
    )!!

    override fun get() = MexDedupProcessor<K>(storeName)
}
