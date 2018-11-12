package com.madhouse.dsp.utils

import com.madhouse.dsp.entity._
import org.apache.flink.configuration.Configuration
import org.apache.flink.streaming.api.functions.sink.RichSinkFunction

class MysqlSink(logType: String, conf: JDBCConf) extends RichSinkFunction[Record] {
  val REQUEST = "request"
  val IMP = "imp"
  val CLK = "clk"
  val REQUESTFIELDS = "timestamp, project_id, campaign_id, material_id, media_id, adspace_id, reqs, bids, wins, errs, create_timestamp"
  val TRACKERFIELDS = "timestamp, project_id, campaign_id, material_id, media_id, adspace_id, imps, clks, vimps, vclks, income, cost, create_timestamp"

  val VALID = 1L
  val INVALID = 0L
  val NOCOSTINCOME = 0L

  val driver = "com.mysql.jdbc.Driver"
  var connection: java.sql.Connection = _
  var statement: java.sql.Statement = _

  override def open(parameters: Configuration): Unit = {
    Class.forName(driver)
    connection = java.sql.DriverManager.getConnection(conf.url, conf.user, conf.passwd)
    statement = connection.createStatement
  }

  override def invoke(value: Record): Unit = {
    rowToMysql(value)
  }

  override def close(): Unit = {
    connection.close()
  }

  def rowToMysql(row: Record): Unit = {
    val now = System.currentTimeMillis() / 1000
    logType match {
      case REQUEST =>
        //timestamp: Long, projectId: Int, campaignId: Int, creativeId: Int, mediaId: Int, adSpaceId: Int,
        // reqs: Long, bids: Long, wins: Long, errs: Long
        val keys = row.key.split(":")
        val cnt = row.value
        val rbwr = keys(1).toInt match {
          //req, bid, win, err
          case 0 => (VALID * cnt, VALID * cnt, VALID * cnt, INVALID)
          case 1 => (VALID * cnt, INVALID, INVALID, INVALID)
          case 2 => (VALID * cnt, INVALID, INVALID, VALID * cnt)
        }
        println(s"#####begin to save report records of $logType to mysql table: ${conf.table}")
        val fields = REQUESTFIELDS
        val sqlStrPrefix = s"insert into ${conf.table} ($fields) values "
        val value = s"(${keys(0)},${rbwr._1},${rbwr._2},${rbwr._3},${rbwr._4},$now)"
        val sqlStr = sqlStrPrefix + value
        println(s"#####sqlStr = $sqlStr")
        statement.execute(sqlStr)
        println(s"#####all records have been saved to mysql table....")

      case IMP | CLK =>
        //timestamp: Long, projectId: Int, campaignId: Int, creativeId: Int, mediaId: Int, adSpaceId: Int,
        // imps: Long, clks: Long, vimps: Long, vclks: Long, income: Long, cost: Long
        val keys = row.key.split(":")
        val tiic = keys(1).split(",")
        val cnt = row.value
        val icvvic =
        //imps , clks , vimps , vclks, income, cost
          if (tiic(0).equalsIgnoreCase(IMP)) {
            if (tiic(1).toLong == INVALID)
              (VALID * cnt, INVALID, VALID * cnt, INVALID, tiic(2).toLong * cnt, tiic(3).toLong * cnt)
            else
              (VALID * cnt, INVALID, INVALID, INVALID, NOCOSTINCOME, NOCOSTINCOME)
          } else {
            if (tiic(1).toLong == INVALID)
              (INVALID, VALID * cnt, INVALID, VALID * cnt, tiic(2).toLong * cnt * 1000, tiic(3).toLong * cnt * 1000)
            else
              (INVALID, VALID * cnt, INVALID, INVALID, NOCOSTINCOME, NOCOSTINCOME)
          }
        println(s"#####begin to save report records of $logType to mysql table: ${conf.table}")
        val fields = TRACKERFIELDS
        val sqlStrPrefix = s"insert into ${conf.table} ($fields) values "
        val value = s"(${keys(0)},${icvvic._1},${icvvic._2},${icvvic._3},${icvvic._4},${icvvic._5},${icvvic._6},$now)"
        val sqlStr = sqlStrPrefix + value
        println(s"#####sqlStr = $sqlStr")
        statement.execute(sqlStr)
        println(s"#####all records have been saved to mysql table....")
    }
  }
}
