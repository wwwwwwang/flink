package com.madhouse.dsp.utils

import org.apache.flink.api.common.serialization.AbstractDeserializationSchema

object BytesSchema extends AbstractDeserializationSchema[Array[Byte]]{
  override def deserialize(message: Array[Byte]): Array[Byte] = message
}
