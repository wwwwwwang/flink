package com.madhouse.dsp

import java.util.Properties

import com.madhouse.dsp.avro.ClickTrack
import com.madhouse.dsp.entity.{JDBCConf, Record, TrackerReport}
import com.madhouse.dsp.utils.ConfigReader._
import com.madhouse.dsp.utils.Functions._
import com.madhouse.dsp.utils.{AvroUtils, BytesSchema, MysqlSink}
import org.apache.flink.api.common.restartstrategy.RestartStrategies
import org.apache.flink.streaming.api.scala._
import org.apache.flink.streaming.api.windowing.time.Time
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer010

object AuxoClk {
  def main(args: Array[String]): Unit = {
    val CLK = "clk"
    var offset = "latest"

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
    properties.setProperty("group.id", "kafka_flink")
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
        val record = TrackerReport(CLK, timeprocess(t.getTime.toLong, 1800L), t.getProjectid.toInt,
          t.getCid.toInt, t.getCrid.toInt, t.getMediaid.toInt, t.getAdspaceid.toInt,
          t.getInvalid, t.getIncome.toLong, t.getCost.toLong)
        Record(record.toString, 1)
      })
      .keyBy(0)
      .timeWindow(Time.seconds(10))
      .sum(1)

    val jdbcConf = JDBCConf(mysqlUrl, mysqlUser, mysqlPasswd, clkTable)
    println(s"#####jdbcConf = ${jdbcConf.toString}")

    messageStream.addSink(new MysqlSink(CLK, jdbcConf))

    env.execute("Kafka Flink Auxo: clk")
  }
}
