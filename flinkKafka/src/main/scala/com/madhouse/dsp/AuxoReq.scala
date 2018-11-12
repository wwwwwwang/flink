package com.madhouse.dsp

import java.util.Properties

import com.madhouse.dsp.avro.MediaBid
import com.madhouse.dsp.entity.{JDBCConf, Record, RequestReport}
import com.madhouse.dsp.utils.ConfigReader._
import com.madhouse.dsp.utils.Functions._
import com.madhouse.dsp.utils.{AvroUtils, BytesSchema, MysqlSink}
import org.apache.flink.api.common.restartstrategy.RestartStrategies
import org.apache.flink.streaming.api.scala._
import org.apache.flink.streaming.api.windowing.time.Time
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer010

object AuxoReq {
  def main(args: Array[String]): Unit = {

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
    val kafkaConsumer = new FlinkKafkaConsumer010(requestTopic, BytesSchema, properties)

    val messageStream = env
      .addSource(kafkaConsumer)
      .map(r => {
        val t = AvroUtils.decode(r, MediaBid.SCHEMA$).asInstanceOf[MediaBid]
        val record = try {
          val (projectId, campaignId, creativeId) =
            if (t.getResponse != null) (t.getResponse.getProjectid.toInt,
              t.getResponse.getCid.toInt, t.getResponse.getCrid.toInt)
            else (0, 0, 0)
          RequestReport(timeprocess(t.getTime.toLong, 1800L), projectId,
            campaignId, creativeId, t.getRequest.getMediaid.toInt,
            t.getRequest.getAdspaceid.toInt, requesStatus(t.getStatus))
        } catch {
          case e: Throwable => println(s"#####get information from ${t.toString}, exception happened:$e")
            RequestReport(timeprocess(t.getTime.toLong, 1800L), 0, 0, 0, 0, 0, requesStatus(0))
        }
        Record(record.toString, 1)
      })
      .keyBy(0)
      .timeWindow(Time.seconds(10))
      .sum(1)

    val jdbcConf = JDBCConf(mysqlUrl, mysqlUser, mysqlPasswd, requestTable)
    println(s"#####jdbcConf = ${jdbcConf.toString}")

    messageStream.addSink(new MysqlSink("request", jdbcConf))

    env.execute("Kafka Flink Auxo: req")
  }
}
