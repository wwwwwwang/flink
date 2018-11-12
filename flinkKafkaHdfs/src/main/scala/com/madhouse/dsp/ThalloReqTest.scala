package com.madhouse.dsp

import java.util.Properties

import com.madhouse.dsp.avro.MediaBid
import com.madhouse.dsp.utils.ConfigReader._
import com.madhouse.dsp.utils._
import org.apache.avro.generic.GenericRecord
import org.apache.flink.api.common.restartstrategy.RestartStrategies
import org.apache.flink.core.fs.Path
import org.apache.flink.streaming.api.functions.sink.filesystem.StreamingFileSink
import org.apache.flink.streaming.api.scala._
//import org.apache.flink.streaming.api.functions.sink.filesystem.StreamingFileSink
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer010

object ThalloReqTest {
  def main(args: Array[String]): Unit = {
    val REQ = "req"
    var offset = requestStartOffset

    if (args.length > 0 && args(0).equalsIgnoreCase("-e")) {
      offset = "earliest"
    }

    val env = StreamExecutionEnvironment.getExecutionEnvironment
    env.getConfig.disableSysoutLogging
    env.getConfig.setRestartStrategy(RestartStrategies.fixedDelayRestart(4, 10000))
    // create a checkpoint every 5 seconds
    env.enableCheckpointing(5000)

    println(s"#####kafka bootstrap servers: $bootstrapServers")

    val properties = new Properties()
    properties.setProperty("bootstrap.servers", bootstrapServers)
    properties.setProperty("group.id", "kafka_flink_thallo")
    properties.setProperty("key.deserializer", "org.apache.kafka.common.serialization.StringDeserializer")
    properties.setProperty("value.deserializer", "org.apache.kafka.common.serialization.ByteArrayDeserializer")
    properties.setProperty("metadata.max.age.ms", "30000")
    //properties.put("enable.auto.commit", "false")
    properties.setProperty("auto.offset.reset", offset)

    // create a Kafka streaming source consumer for Kafka 0.10.x
    val kafkaConsumer = new FlinkKafkaConsumer010(requestTopic, BytesSchema, properties)

    val messageStream = env
      .addSource(kafkaConsumer)
      .map(r => {
        val a = AvroUtils.decode(r, MediaBid.SCHEMA$).asInstanceOf[GenericRecord]
        /*val ts = timeprocess(a.getTime.toLong, 1800L)
        /*val jsonString =
          if (a.getResponse == null) a.toString.replaceAll(", \"response\": null", "") else a.toString
        Record(ts, jsonString)*/
        R(ts, a)*/
        a
      })

    /*val sink = new BucketingSink[R](s"$hdfsBasePath/$REQ")
    sink.setBucketer(new TimestampBucketer[R]("R"))
    sink.setWriter(new ParquetSinkWriter[R]("req"))
    //sink.setWriter(new StringSinkWriter[Record]())
    //sink.setBatchSize(1024 * 1024 * 400) // this is 400 MB,
    //sink.setInactiveBucketThreshold(10 * 60 * 1000); // this is 30 mins*/

    /*val sink = new BucketingSink[GenericRecord]("/base/path")
    sink.setBucketer(new DateTimeBucketer[GenericRecord]("yyyy-MM-dd--HHmm"))
    //sink.setWriter(new SequenceFileWriter[IntWritable, Text]())
    sink.setWriter(ParquetAvroWriters.forGenericRecord(MediaBid.SCHEMA$))
    sink.setBatchSize(1024 * 1024 * 400) // this is 400 MB,*/

    val s = StreamingFileSink.forBulkFormat(
      new Path(s"$hdfsBasePath/$REQ"),
      MyParquetWriters.forGenericRecord(MediaBid.SCHEMA$))
      .build()

    //new StreamingFileSink.BulkFormatBuilder(new Path(s"$hdfsBasePath/$REQ"),)

    messageStream.addSink(s)

    env.execute("Kafka Flink thallo: req")
  }
}
