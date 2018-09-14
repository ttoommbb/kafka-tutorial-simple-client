package com.ippontech.kafkatutorials.simpleclient

import org.apache.kafka.streams.processor.AbstractProcessor
import org.apache.kafka.streams.processor.ProcessorContext
import org.apache.kafka.streams.state.KeyValueStore
import java.nio.ByteBuffer
import java.nio.ByteOrder

class MexDedupProcessor<K>(private val storeName: String) : AbstractProcessor<K, ByteBuffer>() {

    private var stateStore: KeyValueStore<Short, Any>? = null

    override fun init(context: ProcessorContext) {
        println("MexDedupProcessor.init")
        super.init(context)
        val store = context.getStateStore(storeName)
        stateStore = store as? KeyValueStore<Short, Any>?
    }

    override fun process(key: K, value: ByteBuffer?) {
        println("MexDedupProcessor.process")
        value ?: return
        val stateStore = this.stateStore ?: return
        value.order(ByteOrder.LITTLE_ENDIAN)
        val typeIndicator = value.short

        val sequence: Any = when (typeIndicator) {
            ACTION_MESSAGE_INDICATOR -> value.long
            BLOCK_MESSAGE_INDICATOR -> value.int
            else -> throw Exception("Unsupported type of $typeIndicator")
        }
        val last = stateStore.putIfAbsent(typeIndicator, sequence)
        if (last == sequence) {
            return //skip dup
        }
        context().forward(key, value)
    }

    companion object {
        const val ACTION_MESSAGE_INDICATOR: Short = 1
        const val BLOCK_MESSAGE_INDICATOR: Short = 2
    }
}