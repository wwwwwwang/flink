package com.madhouse.dsp.utils

import com.madhouse.dsp.entity._
import com.madhouse.dsp.utils.Constant._
import com.madhouse.dsp.utils.Functions.mkOnDuplicateString
import org.apache.flink.api.common.io.OutputFormat
import org.apache.flink.configuration.Configuration

import scala.collection.mutable.ArrayBuffer

class MysqlOut[T](logType: String, conf: JDBCConf, tableName: String) extends OutputFormat[T] {
  val driver = "com.mysql.jdbc.Driver"
  var connection: java.sql.Connection = _
  var statement: java.sql.Statement = _
  val r: ArrayBuffer[String] = new ArrayBuffer[String]()

  override def configure(parameters: Configuration): Unit = {
    Class.forName(driver)
  }

  override def open(taskNumber: Int, numTasks: Int): Unit = {
    connection = java.sql.DriverManager.getConnection(conf.url, conf.user, conf.passwd)
    statement = connection.createStatement
  }

  override def writeRecord(record: T): Unit = {
    //rowToMysql(record)
    addRowToArray(record)
  }

  override def close(): Unit = {
    arrayToMysql(r)
    connection.close()
    r.clear()
  }

  def addRowToArray(record: T): ArrayBuffer[String] = {
    val str = logType match {
      case REQCAM =>
        val r = record.asInstanceOf[RequestCampaign]
        s"(${r.timestamp},${r.project_id},${r.campaign_id},${r.material_id},${r.bids},${r.wins})"

      case REQCAML=>
        val r = record.asInstanceOf[RequestCampaignWithLocation]
        s"(${r.timestamp},${r.project_id},${r.campaign_id},${r.material_id},${r.location},${r.bids},${r.wins})"

      case REQMED=>
        val r = record.asInstanceOf[RequestMedia]
        s"(${r.timestamp},${r.media_id},${r.adspace_id},${r.reqs},${r.bids},${r.wins},${r.errs})"

      case REQMEDL=>
        val r = record.asInstanceOf[RequestMediaWithLocation]
        s"(${r.timestamp},${r.media_id},${r.adspace_id},${r.location},${r.reqs},${r.bids},${r.wins},${r.errs})"

      case TRACAM =>
        val r = record.asInstanceOf[TrackCampaign]
        s"(${r.timestamp},${r.project_id},${r.campaign_id},${r.material_id},${r.imps},${r.clks},${r.vimps},${r.vclks},${r.cost})"

      case TRACAML=>
        val r = record.asInstanceOf[TrackCampaignWithLocation]
        s"(${r.timestamp},${r.project_id},${r.campaign_id},${r.material_id},${r.location},${r.imps},${r.clks},${r.vimps},${r.vclks},${r.cost})"

      case TRAMED=>
        val r = record.asInstanceOf[TrackMedia]
        s"(${r.timestamp},${r.media_id},${r.adspace_id},${r.imps},${r.clks},${r.vimps},${r.vclks},${r.income})"

      case TRAMEDL=>
        val r = record.asInstanceOf[TrackMediaWithLocation]
        s"(${r.timestamp},${r.media_id},${r.adspace_id},${r.location},${r.imps},${r.clks},${r.vimps},${r.vclks},${r.income})"
    }
    r += str
    r
  }

  def arrayToMysql(r: ArrayBuffer[String]): Unit = {
    println(s"#####begin to save report records of $logType to mysql table: $tableName")
    val sqlStrPrefix = s"insert into $tableName "
    val (fields,duplicate) = logType match {
      case REQCAM =>
        (s"($CAM_COLS,$REQCAM_COLS)",mkOnDuplicateString(tableName, REQCAM_COLS))
      case REQCAML =>
        (s"($CAML_COLS,$REQCAM_COLS)",mkOnDuplicateString(tableName, REQCAM_COLS))
      case REQMED =>
        (s"($MED_COLS,$REQMED_COLS)",mkOnDuplicateString(tableName, REQMED_COLS))
      case REQMEDL =>
        (s"($MEDL_COLS,$REQMED_COLS)",mkOnDuplicateString(tableName, REQMED_COLS))
      case TRACAM =>
        (s"($CAM_COLS,$TRACAM_COLS)",mkOnDuplicateString(tableName, TRACAM_COLS))
      case TRACAML =>
        (s"($CAML_COLS,$TRACAM_COLS)",mkOnDuplicateString(tableName, TRACAM_COLS))
      case TRAMED =>
        (s"($MED_COLS,$TRAMED_COLS)",mkOnDuplicateString(tableName, TRAMED_COLS))
      case TRAMEDL =>
        (s"($MEDL_COLS,$TRAMED_COLS)",mkOnDuplicateString(tableName, TRAMED_COLS))
    }

    r.grouped(100).foreach(arr => {
      val values = arr.mkString(",")
      val sqlStr = s"$sqlStrPrefix $fields values $values ON DUPLICATE KEY UPDATE $duplicate"
      println(s"#####sqlStr = $sqlStr")
      statement.execute(sqlStr)
    })

    println(s"#####all records have been saved to mysql table....")
  }
}
