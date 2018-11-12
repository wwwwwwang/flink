package com.madhouse.dsp

import java.util.Properties

import com.madhouse.dsp.avro.MediaBid
import com.madhouse.dsp.entity.Record
import com.madhouse.dsp.utils.ConfigReader._
import com.madhouse.dsp.utils.Functions._
import com.madhouse.dsp.utils.{AvroUtils, BytesSchema, StringSinkWriter, TimestampBucketer}
import org.apache.flink.api.common.restartstrategy.RestartStrategies
import org.apache.flink.streaming.api.scala._
import org.apache.flink.streaming.connectors.fs.bucketing.BucketingSink
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer010

object ThalloReq {
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
        val a = AvroUtils.decode(r, MediaBid.SCHEMA$).asInstanceOf[MediaBid]
        val ts = timeprocess(a.getTime.toLong, 1800L)
        val jsonString =
          if (a.getResponse == null) a.toString.replaceAll(", \"response\": null", "") else a.toString
        Record(ts, jsonString)
      })

    val sink = new BucketingSink[Record](s"$hdfsBasePath/$REQ")
    sink.setBucketer(new TimestampBucketer[Record]("Record"))
    sink.setWriter(new StringSinkWriter[Record]())
    //sink.setBatchSize(1024 * 1024 * 400) // this is 400 MB,
    //sink.setInactiveBucketThreshold(10 * 60 * 1000); // this is 30 mins

    messageStream.addSink(sink)

    env.execute("Kafka Flink thallo: req")
  }
}
