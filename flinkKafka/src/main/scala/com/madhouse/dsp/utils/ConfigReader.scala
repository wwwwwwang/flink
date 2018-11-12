package com.madhouse.dsp.utils

import java.io.{File, InputStreamReader}
import java.net.URI

import com.typesafe.config.{Config, ConfigFactory}
import org.apache.commons.logging.{Log, LogFactory}
import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.{FSDataInputStream, FileSystem, Path}

/**
  * Created by Madhouse on 2017/12/25.
  */
object ConfigReader extends Serializable {
  var config: Config = _
  val logConfig: Log = LogFactory.getLog(ConfigReader.getClass)

  var defaultHdfsPath = "/travelmad/apps/flink/auxo"
  var path: String = "application.conf"
  var rootName: String = "app"

  def inputStream2String(is: FSDataInputStream): String = {
    scala.io.Source.fromInputStream(is).getLines().mkString("\n")
  }

  def init(configName: String, rootName: String): Unit = {
    logConfig.info(s"#####config file's path = $defaultHdfsPath")
    val directory = new File("..")
    val filePath = directory.getAbsolutePath
    logConfig.info(s"#####directory.getAbsolutePath = $filePath")
    val localPath = filePath.substring(filePath.indexOf(":") + 1, filePath.lastIndexOf("/") + 1) + configName
    logConfig.info(s"#####path = $localPath")
    val configFile = new File(localPath)
    if (configFile.exists()) {
      config = ConfigFactory.parseFile(configFile).getConfig(rootName)
    } else {
      logConfig.info(s"####Property file not found:$localPath, try to get it from hdfs...")

      val hdfsPath = defaultHdfsPath + "/" + configName
      logConfig.info(s"#####start to read config($hdfsPath) file from hdfs")
      val conf: Configuration = new Configuration
      conf.setBoolean("fs.hdfs.impl.disable.cache", true)
      val fs = FileSystem.get(URI.create(hdfsPath), conf)
      if (fs.exists(new Path(hdfsPath))) {
        val in = fs.open(new Path(hdfsPath))
        /*val str = inputStream2String(in)
        log.info(s"#####string = $str")*/
        config = ConfigFactory.parseReader(new InputStreamReader(in)).getConfig(rootName)
        //config = ConfigFactory.parseString(inputStream2String(in)).getConfig(rootName)
        in.close()
        fs.close()
        logConfig.info(s"#####config added from config file...")
      } else {
        logConfig.info(s"####$hdfsPath in hdfs is not exist, cannot get config and exit...")
        fs.close()
        sys.exit(1)
      }
    }
  }

  def getWithElse[T](path: String, defaultValue: T): T = {
    if (config.hasPath(path)) {
      defaultValue match {
        case _: Int => config.getInt(path).asInstanceOf[T]
        case _: String => config.getString(path).asInstanceOf[T]
        case _: Double => config.getDouble(path).asInstanceOf[T]
        case _: Long => config.getLong(path).asInstanceOf[T]
        case _: Boolean => config.getBoolean(path).asInstanceOf[T]
        case _ => defaultValue
      }
    } else {
      defaultValue
    }
  }

  val configDefault: Unit = init(path, rootName)

  val bootstrapServers: String = getWithElse("kafka.bootstrap_servers", "")
  val mysqlUrl: String = getWithElse("mysql.url", "jdbc:mysql://172.16.25.128:3306/travelmad")
  val mysqlUser: String = getWithElse("mysql.user", "travelmaduser")
  val mysqlPasswd: String = getWithElse("mysql.pwd", "madhouse")

  val requestStartOffset: String = getWithElse("request.starting_offsets","latest")
  val requestTopic :String = getWithElse("request.topic_name", "test_tvl_request")
  val requestTable: String = getWithElse("request.save_table", "tvl_report_request_mem")

  val impStartOffset: String = getWithElse("imp.starting_offsets","latest")
  val impTopic :String = getWithElse("imp.topic_name", "test_tvl_imp")
  val impTable: String = getWithElse("imp.save_table", "tvl_report_tracker_mem")

  val clkStartOffset: String = getWithElse("clk.starting_offsets","latest")
  val clkTopic :String = getWithElse("clk.topic_name", "test_tvl_clk")
  val clkTable: String = getWithElse("clk.save_table", "tvl_report_tracker_mem")

}
