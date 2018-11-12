package com.madhouse.dsp.utils

import com.madhouse.dsp.entity.R
import org.apache.avro.Schema
import org.apache.avro.generic.{GenericData, GenericRecord}
import org.apache.flink.api.common.serialization.BulkWriter
import org.apache.flink.core.fs.FSDataOutputStream
import org.apache.flink.formats.parquet.{ParquetBuilder, StreamOutputFile}
import org.apache.flink.util.Preconditions.checkNotNull
import org.apache.parquet.avro.AvroParquetWriter
import org.apache.parquet.hadoop.ParquetWriter
import org.apache.parquet.hadoop.metadata.CompressionCodecName
import org.apache.parquet.io.OutputFile

class MyParquetBulkWriter[T](schema: Schema) extends BulkWriter[T] {
  var parquetWriter: ParquetWriter[GenericRecord] = _
  val compressionCodecName: CompressionCodecName = CompressionCodecName.SNAPPY
  val pageSize: Int = 64 * 1024

  this (parquetWriter: ParquetWriter[GenericRecord]) {
    checkNotNull(parquetWriter, "parquetWriter")
  }

  override def addElement(element: T): Unit = {
    val e = element.asInstanceOf[R]
    parquetWriter.write(e.value)
  }

  override def flush(): Unit = {
    //nothing we can do here
  }

  override def finish(): Unit = {
    parquetWriter.close()
  }

  override def create(stream: FSDataOutputStream): BulkWriter[T] = {
    val out: OutputFile = new StreamOutputFile(stream)
    stream.

    val writerBuilder: ParquetBuilder[GenericRecord]

    val builder = ParquetWriter[T] createWriter(out)

    AvroParquetWriter[GenericRecord].builder(Path(out))
      .withSchema(schema)
      .withDataModel(new GenericData())
      .withCompressionCodec(compressionCodecName)
      .withPageSize(pageSize)
      .build()
  }
}
