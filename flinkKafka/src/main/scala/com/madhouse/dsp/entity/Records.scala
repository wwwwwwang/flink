package com.madhouse.dsp.entity

/**
  * Created by Madhouse on 2017/12/25.
  */
case class Record(key: String, value: Int)

case class JDBCConf(url: String, user: String, passwd: String, table: String) {
  override def toString: String = {
    s"url=$url, user=$user, passwd=$passwd,table=$table"
  }
}

case class RequestReport(timestamp: Long, projectId: Int, campaignId: Int,
                         creativeId: Int, mediaId: Int, adSpaceId: Int, status: Int) {
  override def toString: String = {
    s"$timestamp,$projectId,$campaignId,$creativeId,$mediaId,$adSpaceId:$status"
  }
}

case class TrackerReport(logType: String, timestamp: Long, projectId: Int, campaignId: Int,
                         creativeId: Int, mediaId: Int,
                         adSpaceId: Int, invalid: Int, income: Long, cost: Long) {
  override def toString: String = {
    s"$timestamp,$projectId,$campaignId,$creativeId,$mediaId,$adSpaceId:" +
      s"$logType,$invalid,$income,$cost"
  }
}