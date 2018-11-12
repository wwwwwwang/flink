package com.madhouse.dsp

import java.util.Properties

import com.madhouse.dsp.avro.ClickTrack
import com.madhouse.dsp.entity.Record
import com.madhouse.dsp.utils.ConfigReader._
import com.madhouse.dsp.utils.Functions._
import com.madhouse.dsp.utils.{AvroUtils, BytesSchema, StringSinkWriter, TimestampBucketer}
import org.apache.flink.api.common.restartstrategy.RestartStrategies
import org.apache.flink.streaming.api.scala._
import org.apache.flink.streaming.connectors.fs.StringWriter
import org.apache.flink.streaming.connectors.fs.bucketing.{BucketingSink, DateTimeBucketer}
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer010

object ThalloClk {
  def main(args: Array[String]): Unit = {
    val CLK = "clk"
    var offset = clkStartOffset

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
    val kafkaConsumer = new FlinkKafkaConsumer010(clkTopic, BytesSchema, properties)

    val messageStream = env
      .addSource(kafkaConsumer)
      .map(r => {
        val t = AvroUtils.decode(r, ClickTrack.SCHEMA$).asInstanceOf[ClickTrack]
        val ts = timeprocess(t.getTime.toLong, 1800L)
        Record(ts, t.toString)
      })

    val sink = new BucketingSink[Record](s"$hdfsBasePath/$CLK")
    sink.setBucketer(new TimestampBucketer[Record]("Record"))
    sink.setWriter(new StringSinkWriter[Record]())
    //sink.setBatchSize(1024 * 1024 * 400) // this is 400 MB,
    //sink.setInactiveBucketThreshold(10 * 60 * 1000); // this is 30 mins

    messageStream.addSink(sink)

    env.execute("Kafka Flink thallo: clk")
  }
}
