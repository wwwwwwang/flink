package com.madhouse.dsp.utils

import org.apache.avro.Schema
import org.apache.avro.generic.GenericRecord
import org.apache.avro.io.DecoderFactory
import org.apache.avro.reflect.ReflectDatumReader

/**
  * Created by Madhouse on 2017/12/25.
  */
object AvroUtils extends Serializable {
  def decode(bytes: Array[Byte], writer: Schema, reader: Schema): Any = {
    val decoder = new ReflectDatumReader[GenericRecord](writer, reader)
    decoder.read(null.asInstanceOf[GenericRecord], DecoderFactory.get.binaryDecoder(bytes, null))
  }
  def decode(bytes: Array[Byte], reader: Schema): Any = {
    decode(bytes, reader, reader)
  }
}
