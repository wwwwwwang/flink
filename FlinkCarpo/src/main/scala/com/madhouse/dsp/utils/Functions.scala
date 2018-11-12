package com.madhouse.dsp.utils

import java.net.URI
import java.sql.Timestamp
import java.time.format.DateTimeFormatter.ofPattern
import java.time.{LocalDateTime, ZoneId}

import com.madhouse.dsp.utils.ConfigReader.hdfsBasePath
import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.{FileSystem, Path}

import scala.collection.mutable.ArrayBuffer

/**
  * Created by Madhouse on 2017/12/27.
  */
object Functions extends Serializable {
  val ZONE: ZoneId = ZoneId.of("Asia/Shanghai")
  val PATTERN = "yyyyMMddHHmm"
  val HALFHOUR = 1800
  val MINUTE = 60

  def timeProcess(time: Long, cell: Long): Long = {
    time.toString.length match {
      case 10 => time - time % cell
      case 13 => val sencond = time / 1000
        sencond - sencond % cell
    }
  }

  def requesStatus(status: Int): Int = {
    status match {
      case 200 => 0
      case 204 => 1
      case 400 | 500 => 2
      case _ => 1
    }
  }

  def string2DateTime(s: String): LocalDateTime = {
    val now = LocalDateTime.now(ZONE)
    val dateTime = if (s.trim.equalsIgnoreCase("")) now else {
      s.length match {
        case 8 => LocalDateTime.parse(s + "0000", ofPattern(PATTERN))
        case 12 => LocalDateTime.parse(s, ofPattern(PATTERN))
      }
    }
    dateTime
  }

  def dateTime2Long(l: LocalDateTime): Long = {
    val t = Timestamp.from(l.atZone(ZONE).toInstant)
    timeProcess(t.getTime, HALFHOUR)
  }

  def dealStartAndEnd(s: String, e: String): ArrayBuffer[Long] = {
    val timestamps: ArrayBuffer[Long] = ArrayBuffer[Long]()
    val start = dateTime2Long(string2DateTime(s))
    val end = if (e.trim.equalsIgnoreCase("")) start else dateTime2Long(string2DateTime(e))
    val cnt = (end - start) / HALFHOUR
    for (i <- 0 to cnt.toInt) {
      timestamps += start + HALFHOUR * i
    }
    timestamps
  }

  def addPrefix(prefix:String, cols:String):String ={
    var res = ""
    val cs = cols.split(",")
    for(c <- cs){
      res += s"$c as ${prefix}_$c,"
    }
    res.dropRight(1)
  }

  def mkOnDuplicateString(table:String, cols:String):String ={
    var res = ""
    val cs = cols.split(",")
    for(c <- cs){
      res += s"$table.$c=$table.$c + VALUES($c),"
    }
    res.dropRight(1)
  }

  def makeSumString(cols:String):String ={
    var res = ""
    val cs = cols.split(",")
    for(c <- cs){
      res += s"sum($c) as $c,"
    }
    res.dropRight(1)
  }

  def removePrefix(prefix:String, cols:String):String ={
    var res = ""
    val cs = cols.split(",")
    for(c <- cs){
      res += s"${prefix}_$c as $c,"
    }
    res.dropRight(1)
  }

  def mkJoinString(l:String, r:String, cols:String):String ={
    var res = ""
    val cs = cols.split(",")
    for(c <- cs){
      res += s"${l}_$c = ${r}_$c&&"
    }
    res.dropRight(2)
  }

  def mkString(subStrs: String*): String = {
    subStrs.mkString("/")
  }

  def isExistHdfsPath(s: String): ArrayBuffer[Boolean] = {
    val res: ArrayBuffer[Boolean] = ArrayBuffer[Boolean]()
    val path = hdfsBasePath
    val conf: Configuration = new Configuration
    conf.setBoolean("fs.hdfs.impl.disable.cache", true)
    val fs = FileSystem.get(URI.create(path), conf)
    var req = false
    var imp = false
    var clk = false
    if (fs.exists(new Path(mkString(hdfsBasePath, "req", s)))) {
      req = true
    }
    if (fs.exists(new Path(mkString(hdfsBasePath, "imp", s)))) {
      imp = true
    }
    if (fs.exists(new Path(mkString(hdfsBasePath, "clk", s)))) {
      clk = true
    }
    res += req
    res += imp
    res += clk
    res
  }

  def isExistHdfsPath(s: String, t:String): Boolean = {
    val path = hdfsBasePath
    val conf: Configuration = new Configuration
    conf.setBoolean("fs.hdfs.impl.disable.cache", true)
    val fs = FileSystem.get(URI.create(path), conf)
    fs.exists(new Path(mkString(hdfsBasePath, t, s)))
  }

  def getOrDefault(i:Integer): Integer={
    if(i==null) 0 else i
  }

}
