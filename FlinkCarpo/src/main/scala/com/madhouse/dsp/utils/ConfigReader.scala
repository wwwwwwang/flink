package com.madhouse.dsp.utils

import java.io.{File, InputStreamReader}
import java.net.URI
import java.util.Properties

import com.typesafe.config.{Config, ConfigFactory}
import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.{FSDataInputStream, FileSystem, Path}

/**
  * Created by Madhouse on 2017/12/25.
  */
object ConfigReader extends Serializable {
  var config: Config = _

  var defaultHdfsPath = "/travelmad/apps/flink/carpo"
  var path: String = "application.conf"
  var rootName: String = "app"

  def inputStream2String(is: FSDataInputStream): String = {
    scala.io.Source.fromInputStream(is).getLines().mkString("\n")
  }

  def init(configName: String, rootName: String): Unit = {
    //println(s"#####config file's path = $defaultHdfsPath")
    val directory = new File("..")
    val filePath = directory.getAbsolutePath
    //println(s"#####directory.getAbsolutePath = $filePath")
    val localPath = filePath.substring(filePath.indexOf(":") + 1, filePath.lastIndexOf("/") + 1) + configName
    //println(s"#####path = $localPath")
    val configFile = new File(localPath)
    if (configFile.exists()) {
      config = ConfigFactory.parseFile(configFile).getConfig(rootName)
    } else {
      println(s"####Property file not found:$localPath, try to get it from hdfs...")

      val hdfsPath = defaultHdfsPath + "/" + configName
      println(s"#####start to read config($hdfsPath) file from hdfs")
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
        println(s"#####config added from config file...")
      } else {
        println(s"####$hdfsPath in hdfs is not exist, cannot get config and exit...")
        fs.close()
        sys.exit(1)
      }
    }
  }

  def getWithDefault[T](path: String, defaultValue: T): T = {
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

  val hdfsPath: String = getWithDefault("hdfs.base_path", "/travelmad/flink/applogs/")
  val hdfsBasePath: String = if (hdfsPath.endsWith("/")) hdfsPath.dropRight(1) else hdfsPath

  val mysqlUrl: String = getWithDefault("mysql.url", "jdbc:mysql://172.16.25.128:3306/travelmad")
  val mysqlUser: String = getWithDefault("mysql.user", "travelmaduser")
  val mysqlPasswd: String = getWithDefault("mysql.pwd", "madhouse")
  val connectionProperties = new Properties()
  connectionProperties.setProperty("user", mysqlUser)
  connectionProperties.setProperty("password", mysqlPasswd)
  connectionProperties.setProperty("driver", "com.mysql.jdbc.Driver")
  val campaignTable: String = getWithDefault("mysql.campaign_table", "tvl_report_campaign_mem")
  val campaignTableLocation: String = getWithDefault("mysql.campaign_table_location", "tvl_report_campaign_location_mem")
  val mediaTable: String = getWithDefault("mysql.media_table","tvl_report_media_mem")
  val mediaTableLocation: String = getWithDefault("mysql.media_table_location","tvl_report_media_location_mem")
  val campaignTablePathch: String = getWithDefault("mysql.campaign_table_patch", "tvl_report_campaign_patch")
  val campaignTableLocationPatch: String = getWithDefault("mysql.campaign_table_location_patch", "tvl_report_campaign_location_patch")
  val mediaTablePatch: String = getWithDefault("mysql.media_table_patch","tvl_report_media_patch")
  val mediaTableLocationPatch: String = getWithDefault("mysql.media_table_location_patch","tvl_report_media_location_patch")
}
