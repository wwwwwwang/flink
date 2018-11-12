package com.madhouse.dsp.utils

import com.madhouse.dsp.entity.Record
import org.apache.flink.streaming.connectors.fs.{StreamWriterBase, Writer}

class StringSinkWriter[T] extends StreamWriterBase[T] {
  override def write(element: T): Unit = {
    val e = element.asInstanceOf[Record]
    val outputStream = getStream
    outputStream.write(e.value.toString.getBytes("UTF-8"))
    outputStream.write('\n')
  }

  override def duplicate(): Writer[T] = new StringSinkWriter[T]
}
