package com.madhouse.dsp

import com.madhouse.dsp.entity.{RequestRecord, TrackRecord}
import com.madhouse.dsp.utils.ConfigReader._
import com.madhouse.dsp.utils.Functions._
import org.apache.flink.api.scala.ExecutionEnvironment
import org.apache.flink.streaming.api.scala._
import org.apache.flink.table.api.TableEnvironment
import org.apache.flink.types.Row

import scala.util.parsing.json.JSON

/**
  * Created by Madhouse on 2018/1/12.
  */
object CarpoProcess extends Serializable {

  //def process(env:ExecutionEnvironment, ts: Long, patch: Boolean = false): Unit = {
  def process(ts: Long, patch: Boolean = false): Unit = {

    println(s"#####start to process job with start time: $ts")

    val isExists = isExistHdfsPath(ts.toString)
    if (!(isExists(0) || isExists(1) || isExists(2))) {
      println(s"#####all(req,imp,clk) has no record logs in hdfs of half hour begins with $ts, exit....")
    } else {
      /*try {*/
      val env = ExecutionEnvironment.getExecutionEnvironment
      val tableEnv = TableEnvironment.getTableEnvironment(env)

      val reqDataset = if (isExists(0)) {
        println(s"#####request has record logs in hdfs of half hour begins with $ts")
        val reqPath = mkString(hdfsBasePath, "req", ts.toString)
        env.readTextFile(reqPath).map(r => {
          /*val j = jsonParser.parse(r).asInstanceOf[JSONObject]
          RequestRecord(ts, j.getOrDefault("response.projectid", "0").asInstanceOf[Int],
            j.getOrDefault("response.cid", "0").asInstanceOf[Int],
            j.getOrDefault("response.crid", "0").asInstanceOf[Int],
            j.getOrDefault("request.mediaid", "0").asInstanceOf[Int],
            j.getOrDefault("request.adspaceid", "0").asInstanceOf[Int],
            j.getOrDefault("request.location", "0").asInstanceOf[String],
            j.getOrDefault("status", "0").asInstanceOf[Int])*/
          val j = JSON.parseFull(r)
          RequestRecord(ts, j.getOrElse("response.projectid", "0").asInstanceOf[Int],
            j.getOrElse("response.cid", "0").asInstanceOf[Int],
            j.getOrElse("response.crid", "0").asInstanceOf[Int],
            j.getOrElse("request.mediaid", "0").asInstanceOf[Int],
            j.getOrElse("request.adspaceid", "0").asInstanceOf[Int],
            j.getOrElse("request.location", "0").asInstanceOf[String],
            j.getOrElse("status", "0").asInstanceOf[Int])
        })
      } else {
        println(s"#####request has no record logs in hdfs of half hour begins with $ts, using a null dataframe to replace")
        env.fromCollection(Seq(RequestRecord(0L, 0, 0, 0, 0, 0, "", 0)))
      }
      val req = reqDataset.filter(r => r.timestamp > 0)
      //println(s"#####request has ${req.count()} records..")

      val impDataset = if (isExists(1)) {
        println(s"#####imp has record logs in hdfs of half hour begins with $ts")
        val impPath = mkString(hdfsBasePath, "imp", ts.toString)
        env.readTextFile(impPath).map(r => {
          /*val j = jsonParser.parse(r).asInstanceOf[JSONObject]
          TrackRecord("imp", ts, j.getOrDefault("projectid", "0").asInstanceOf[Int],
            j.getOrDefault("cid", "0").asInstanceOf[Int],
            j.getOrDefault("crid", "0").asInstanceOf[Int],
            j.getOrDefault("mediaid", "0").asInstanceOf[Int],
            j.getOrDefault("adspaceid", "0").asInstanceOf[Int],
            j.getOrDefault("location", "0").asInstanceOf[String],
            j.getOrDefault("invalid", "0").asInstanceOf[Int],
            j.getOrDefault("income", "0").asInstanceOf[Long],
            j.getOrDefault("cost", "0").asInstanceOf[Long])*/
          val j = JSON.parseFull(r)
          TrackRecord("imp", ts, j.getOrElse("projectid", "0").asInstanceOf[Int],
            j.getOrElse("cid", "0").asInstanceOf[Int],
            j.getOrElse("crid", "0").asInstanceOf[Int],
            j.getOrElse("mediaid", "0").asInstanceOf[Int],
            j.getOrElse("adspaceid", "0").asInstanceOf[Int],
            j.getOrElse("location", "0").asInstanceOf[String],
            j.getOrElse("invalid", "0").asInstanceOf[Int],
            j.getOrElse("income", "0").asInstanceOf[Long],
            j.getOrElse("cost", "0").asInstanceOf[Long])
        })
      } else {
        println(s"#####imp has no record logs in hdfs of half hour begins with $ts, using a null dataframe to replace")
        env.fromCollection(Seq(TrackRecord("imp", 0L, 0, 0, 0, 0, 0, "", 0, 0L, 0L)))
      }
      val imp = impDataset.filter(r => r.timestamp > 0)
      //println(s"#####impression has ${imp.count()} records..")

      val clkDataset = if (isExists(2)) {
        println(s"#####clk has record logs in hdfs of half hour begins with $ts")
        val clkPath = mkString(hdfsBasePath, "clk", ts.toString)
        env.readTextFile(clkPath).map(r => {
          /*val j = jsonParser.parse(r).asInstanceOf[JSONObject]
          TrackRecord("clk", ts, j.getOrDefault("projectid", "0").asInstanceOf[Int],
            j.getOrDefault("cid", "0").asInstanceOf[Int],
            j.getOrDefault("crid", "0").asInstanceOf[Int],
            j.getOrDefault("mediaid", "0").asInstanceOf[Int],
            j.getOrDefault("adspaceid", "0").asInstanceOf[Int],
            j.getOrDefault("location", "0").asInstanceOf[String],
            j.getOrDefault("invalid", "0").asInstanceOf[Int],
            j.getOrDefault("income", "0").asInstanceOf[Long],
            j.getOrDefault("cost", "0").asInstanceOf[Long])*/
          val j = JSON.parseFull(r)
          TrackRecord("clk", ts, j.getOrElse("projectid", "0").asInstanceOf[Int],
            j.getOrElse("cid", "0").asInstanceOf[Int],
            j.getOrElse("crid", "0").asInstanceOf[Int],
            j.getOrElse("mediaid", "0").asInstanceOf[Int],
            j.getOrElse("adspaceid", "0").asInstanceOf[Int],
            j.getOrElse("location", "0").asInstanceOf[String],
            j.getOrElse("invalid", "0").asInstanceOf[Int],
            j.getOrElse("income", "0").asInstanceOf[Long],
            j.getOrElse("cost", "0").asInstanceOf[Long])
        })
      } else {
        println(s"#####clk has no record logs in hdfs of half hour begins with $ts, using a null dataframe to replace")
        env.fromCollection(Seq(TrackRecord("clk", 0L, 0, 0, 0, 0, 0, "", 0, 0L, 0L)))
      }
      val clk = clkDataset.filter(r => r.timestamp > 0)
      //println(s"#####click has ${clk.count()} records..")

      val track = imp.union(clk)

      val reqTable = tableEnv.fromDataSet(req)
      //.groupBy("timestamp, project_id, campaign_id, material_id, media_id, adspace_id, location") //reqs: Long, bids: Long, wins: Long, errs
      //.select("timestamp, project_id, campaign_id, material_id, media_id, adspace_id, location, sum(reqs) as reqs, sum(bids) as bids, sum(wins) as wins, sum(errs) as errs")

      val trackTable = tableEnv.fromDataSet(track)
      //.groupBy("timestamp, project_id, campaign_id, material_id, media_id, adspace_id, location")
      //.select("timestamp, project_id, campaign_id, material_id, media_id, adspace_id, location, sum(imps) as imps, sum(clks) as clks, sum(vimps) as vimps, sum(vclks) as vclks, sum(income) as income, sum(cost) as cost")

      //campaign
      println(s"#####start to process campaign report...")
      val cTable = if (patch) campaignTablePathch else campaignTable
      val joinCols = "timestamp,project_id,campaign_id,material_id"
      val reqCampaignDF = reqTable.groupBy(joinCols)
        .select(s"${addPrefix("r", joinCols)}, sum(bids) as bids, sum(wins) as wins")
      val traCampaignDF = trackTable.groupBy(joinCols)
        .select(s"${addPrefix("t", joinCols)}, sum(imps) as imps, sum(clks) as clks, sum(vimps) as vimps, sum(vclks) as vclks, sum(cost) as cost")

      tableEnv.toDataSet[Row](reqCampaignDF).print()


      /*val selectCols = "bids,wins,imps,clks,vimps,vclks,cost"
      println(s"#####dataframe join by $joinCols, selected columns are $joinCols,$selectCols...")
      val campaignDF = reqCampaignDF.fullOuterJoin(traCampaignDF, mkJoinString("r", "t", joinCols))
        .select(s"${removePrefix("r", joinCols)},$selectCols")

      println(s"#####campaignDF ready to save into table $cTable")

      val resDs = tableEnv.toDataSet[Row](campaignDF)

      resDs.print*/


      /*val sqlString = s"INSERT INTO $cTable ($joinCols,$selectCols) VALUES (?,?,?,?,?,?,?,?,?,?,?)"
      println(s"#####sqlString = $sqlString")
      val sink: JDBCAppendTableSink = JDBCAppendTableSink.builder()
        .setDrivername("com.mysql.jdbc.Driver")
        .setDBUrl(mysqlUrl)
        .setUsername(mysqlUser)
        .setPassword(mysqlPasswd)
        .setBatchSize(100)
        .setQuery(sqlString)
        .setParameterTypes(LONG_TYPE_INFO, INT_TYPE_INFO, INT_TYPE_INFO, INT_TYPE_INFO, LONG_TYPE_INFO,
          LONG_TYPE_INFO, LONG_TYPE_INFO, LONG_TYPE_INFO, LONG_TYPE_INFO, LONG_TYPE_INFO, LONG_TYPE_INFO)
        .build()

      campaignDF.writeToSink(sink)*/

      println(s"#####campaign report processed end...")

      /*
      //campaign with location
      println(s"#####start to process campaign report with location...")
      val cTableL = if (patch) campaignTableLocationPatch else campaignTableLocation
      val reqCampaignLocationDF = requestDeal(reqDF, "RequestCampaignLocationRecord")(spark)
      val traCampaignLocationDF = trackDeal(impDF, clkDF, "TrackCampaignLocationRecord")(spark)
      val joinCols1 = "timestamp,project_id,campaign_id,material_id,location"
      val selectCols1 = "bids,wins,imps,clks,vimps,vclks,cost"
      println(s"#####dataframe join by $joinCols1, selected columns are $joinCols1,$selectCols1...")
      val campaignLocationDF = dfDeal(reqCampaignLocationDF, traCampaignLocationDF)(joinCols1)(selectCols1).filter("project_id > 0").filter("bids>0 or imps>0 or clks>0 or cost>0").cache
      campaignLocationDF.show
      println(s"#####campaignLocationDF has ${campaignLocationDF.count()} records, ready to save into table $cTableL")
      campaignLocationDF.coalesce(4).write.mode("append").jdbc(mysqlUrl, cTableL, connectionProperties)
      campaignLocationDF.unpersist()
      println(s"#####campaign report with location processed end...")

      //media
      println(s"#####start to process media report...")
      val mTable = if (patch) mediaTablePatch else mediaTable
      val reqMediaDF = requestDeal(reqDF, "RequestMediaRecord")(spark)
      val traMediaDF = trackDeal(impDF, clkDF, "TrackMediaRecord")(spark)
      val joinCols2 = "timestamp,media_id,adspace_id"
      val selectCols2 = "reqs,bids,wins,errs,imps,clks,vimps,vclks,income"
      println(s"#####dataframe join by $joinCols2, selected columns are $joinCols2,$selectCols2...")
      val mediaDF = dfDeal(reqMediaDF, traMediaDF)(joinCols2)(selectCols2).filter("media_id > 0").filter("reqs>0 or imps>0 or clks>0 or income>0").cache
      mediaDF.show
      println(s"#####mediaDF has ${mediaDF.count()} records, ready to save into table $mTable")
      mediaDF.coalesce(4).write.mode("append").jdbc(mysqlUrl, mTable, connectionProperties)
      mediaDF.unpersist()
      println(s"#####media report processed end...")

      //media with location
      println(s"#####start to process media report with location...")
      val mTableL = if (patch) mediaTableLocationPatch else mediaTableLocation
      val reqMediaLocationDF = requestDeal(reqDF, "RequestMediaLocationRecord")(spark)
      val traMeidaLocationDF = trackDeal(impDF, clkDF, "TrackMediaLocationRecord")(spark)
      val joinCols3 = "timestamp,media_id,adspace_id,location"
      val selectCols3 = "reqs,bids,wins,errs,imps,clks,vimps,vclks,income"
      println(s"#####dataframe join by $joinCols3, selected columns are $joinCols3,$selectCols3...")
      val mediaLocationDF = dfDeal(reqMediaLocationDF, traMeidaLocationDF)(joinCols3)(selectCols3).filter("media_id > 0").filter("reqs>0 or imps>0 or clks>0 or income>0").cache
      mediaLocationDF.show
      println(s"#####mediaLocationDF has ${mediaLocationDF.count()} records, ready to save into table $mTableL")
      mediaLocationDF.coalesce(4).write.mode("append").jdbc(mysqlUrl, mTableL, connectionProperties)
      mediaLocationDF.unpersist()
      println(s"#####media report with location processed end...")*/

      env.execute(s"flink carpo: $ts")
      /*} catch {
        //case _: IOException => println(s"#####some of following paths are not exist!!\n#####reqPath=$reqPath\n#####impPath=$impPath\n#####clkPath=$clkPath")
        case e: Throwable => e.printStackTrace()
      }*/
    }
    println(s"#####process job with start time: $ts end...")
  }
}
