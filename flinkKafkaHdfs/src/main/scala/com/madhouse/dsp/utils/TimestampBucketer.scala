package com.madhouse.dsp.utils

import com.madhouse.dsp.entity.{R, Record}
import org.apache.hadoop.fs.Path
import org.apache.flink.streaming.connectors.fs.Clock
import org.apache.flink.streaming.connectors.fs.bucketing.Bucketer

class TimestampBucketer[T](t:String) extends Bucketer[T] {
  override def getBucketPath(clock: Clock, basePath: Path, element: T): Path = {
    val p = t match {
      case "R" => element.asInstanceOf[R].key
      case _ => element.asInstanceOf[Record].key
    }
    new Path(s"$basePath/$p")
    //new Path(s"$basePath/${element.asInstanceOf[Record].key}")
  }
}