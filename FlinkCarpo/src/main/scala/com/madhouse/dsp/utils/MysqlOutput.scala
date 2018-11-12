package com.madhouse.dsp.utils

import com.madhouse.dsp.entity._
import com.madhouse.dsp.utils.Functions._
import com.madhouse.dsp.utils.Constant._
import org.apache.flink.api.common.io.OutputFormat
import org.apache.flink.configuration.Configuration

class MysqlOutput[T](logType: String, conf: JDBCConf, table: String) extends  OutputFormat[T]{
  val driver = "com.mysql.jdbc.Driver"
  var connection: java.sql.Connection = _
  var statement: java.sql.Statement = _
  override def configure(parameters: Configuration): Unit = {
    Class.forName(driver)
  }

  override def open(taskNumber: Int, numTasks: Int): Unit = {
    connection = java.sql.DriverManager.getConnection(conf.url, conf.user, conf.passwd)
    statement = connection.createStatement
  }

  override def writeRecord(record: T): Unit = {
    rowToMysql(record)
  }

  override def close(): Unit = {
    connection.close()
  }

  def rowToMysql(record: T):Unit ={
    println(s"#####begin to save report records of $logType to mysql table: $table")
    val sqlStrPrefix = s"insert into $table "
    val sqlStr = logType match {
      case REQCAM =>
        val r = record.asInstanceOf[RequestCampaign]
        val fields = s"($CAM_COLS,$REQCAM_COLS)"
        val value = s"(${r.timestamp},${r.project_id},${r.campaign_id},${r.material_id},${r.bids},${r.wins})"
        val duplicate = mkOnDuplicateString(table, REQCAM_COLS)
        s"$sqlStrPrefix $fields values $value ON DUPLICATE KEY UPDATE $duplicate"

      case REQCAML=>
        val r = record.asInstanceOf[RequestCampaignWithLocation]
        val fields = s"($CAML_COLS,$REQCAM_COLS)"
        val value = s"(${r.timestamp},${r.project_id},${r.campaign_id},${r.material_id},${r.location},${r.bids},${r.wins})"
        val duplicate = mkOnDuplicateString(table, REQCAM_COLS)
        s"$sqlStrPrefix $fields values $value ON DUPLICATE KEY UPDATE $duplicate"

      case REQMED=>
        val r = record.asInstanceOf[RequestMedia]
        val fields = s"($MED_COLS,$REQMED_COLS)"
        val value = s"(${r.timestamp},${r.media_id},${r.adspace_id},${r.reqs},${r.bids},${r.wins},${r.errs})"
        val duplicate = mkOnDuplicateString(table, REQMED_COLS)
        s"$sqlStrPrefix $fields values $value ON DUPLICATE KEY UPDATE $duplicate"

      case REQMEDL=>
        val r = record.asInstanceOf[RequestMediaWithLocation]
        val fields = s"($MEDL_COLS,$REQMED_COLS)"
        val value = s"(${r.timestamp},${r.media_id},${r.adspace_id},${r.location},${r.reqs},${r.bids},${r.wins},${r.errs})"
        val duplicate = mkOnDuplicateString(table, REQMED_COLS)
        s"$sqlStrPrefix $fields values $value ON DUPLICATE KEY UPDATE $duplicate"

      case TRACAM =>
        val r = record.asInstanceOf[TrackCampaign]
        val fields = s"($CAM_COLS,$TRACAM_COLS)"
        val value = s"(${r.timestamp},${r.project_id},${r.campaign_id},${r.material_id},${r.imps},${r.clks},${r.vimps},${r.vclks},${r.cost})"
        val duplicate = mkOnDuplicateString(table, TRACAM_COLS)
        s"$sqlStrPrefix $fields values $value ON DUPLICATE KEY UPDATE $duplicate"

      case TRACAML=>
        val r = record.asInstanceOf[TrackCampaignWithLocation]
        val fields = s"($CAML_COLS,$TRACAM_COLS)"
        val value = s"(${r.timestamp},${r.project_id},${r.campaign_id},${r.material_id},${r.location},${r.imps},${r.clks},${r.vimps},${r.vclks},${r.cost})"
        val duplicate = mkOnDuplicateString(table, TRACAM_COLS)
        s"$sqlStrPrefix $fields values $value ON DUPLICATE KEY UPDATE $duplicate"

      case TRAMED=>
        val r = record.asInstanceOf[TrackMedia]
        val fields = s"($MED_COLS,$TRAMED_COLS)"
        val value = s"(${r.timestamp},${r.media_id},${r.adspace_id},${r.imps},${r.clks},${r.vimps},${r.vclks},${r.income})"
        val duplicate = mkOnDuplicateString(table, TRAMED_COLS)
        s"$sqlStrPrefix $fields values $value ON DUPLICATE KEY UPDATE $duplicate"

      case TRAMEDL=>
        val r = record.asInstanceOf[TrackMediaWithLocation]
        val fields = s"($MEDL_COLS,$TRAMED_COLS)"
        val value = s"(${r.timestamp},${r.media_id},${r.adspace_id},${r.location},${r.imps},${r.clks},${r.vimps},${r.vclks},${r.income})"
        val duplicate = mkOnDuplicateString(table, TRAMED_COLS)
        s"$sqlStrPrefix $fields values $value ON DUPLICATE KEY UPDATE $duplicate"
    }

    println(s"#####sqlStr = $sqlStr")
    statement.execute(sqlStr)
    println(s"#####all records have been saved to mysql table....")
  }
}
