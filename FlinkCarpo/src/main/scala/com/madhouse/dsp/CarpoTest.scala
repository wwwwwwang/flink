package com.madhouse.dsp

import com.alibaba.fastjson.JSON
import com.madhouse.dsp.entity._
import com.madhouse.dsp.utils.ConfigReader._
import com.madhouse.dsp.utils.Constant._
import com.madhouse.dsp.utils.Functions._
import com.madhouse.dsp.utils.{MysqlOut, MysqlOutput}
import org.apache.commons.cli._
import org.apache.flink.api.scala._
import org.apache.flink.table.api.TableEnvironment


/**
  * Created by Madhouse on 2017/12/25.
  */
object CarpoTest {

  def main(args: Array[String]): Unit = {
    var start = ""
    var end = ""
    var patch = false

    val opt = new Options()

    opt.addOption("s", "start", true, "set the start time with format:yyyyMMdd or yyyyMMddHHmm")
    opt.addOption("h", "help", false, "help message")
    opt.addOption("e", "end", true, "set the start time with format:yyyyMMdd or yyyyMMddHHmm")
    opt.addOption("p", "patch", false, "whether use for patching data")

    val formatstr = "sh run.sh mesos ...."
    val formatter = new HelpFormatter
    val parser = new DefaultParser

    var cl: CommandLine = null
    try
      cl = parser.parse(opt, args)
    catch {
      case e: ParseException =>
        e.printStackTrace()
        formatter.printHelp(formatstr, opt)
        System.exit(1)
    }
    if (cl.hasOption("e")) end = cl.getOptionValue("e")
    if (cl.hasOption("h")) {
      formatter.printHelp(formatstr, opt)
      System.exit(0)
    }
    if (cl.hasOption("s")) start = cl.getOptionValue("s")
    if (cl.hasOption("p")) patch = true
    println(s"#####start = $start, end = $end, patch = $patch")

    val startTime = System.currentTimeMillis()
    val jobs = dealStartAndEnd(start, end)
    println(s"there are ${jobs.size} jobs will be done, from: ${jobs.head}, to: ${jobs.last}, interval is half hour")

    val env = ExecutionEnvironment.getExecutionEnvironment
    val tableEnv = TableEnvironment.getTableEnvironment(env)

    val jdbcConf = JDBCConf(mysqlUrl, mysqlUser, mysqlPasswd)
    println(s"#####jdbcConf: ${jdbcConf.toString}")

    //val job = jobs.head
    for (job <- jobs) {
      val reqDataset = if (isExistHdfsPath(job.toString, REQ)) {
        println(s"#####request has record logs in hdfs of half hour begins with $job")

        val reqPath = mkString(HDFS_PREFIX, hdfsBasePath, REQ, job.toString)
        println(s"#####reqpath = $reqPath")
        env.readTextFile(reqPath)
          .map(r => {
            val j = JSON.parseObject(r)
            val response = j.getJSONObject("response")
            val request = j.getJSONObject("request")
            val (pid, cid, crid) = if (response != null) (response.getInteger("projectid").asInstanceOf[Int],
              response.getInteger("cid").asInstanceOf[Int], response.getInteger("crid").asInstanceOf[Int])
            else (0, 0, 0)

            RequestRecord(job, pid, cid, crid,
              request.getInteger("mediaid"),
              request.getInteger("adspaceid"),
              request.getString("location"),
              j.getInteger("status"))
          })
      } else {
        println(s"#####request has no record logs in hdfs of half hour begins with $job, using a null dataframe to replace")
        env.fromCollection(Seq(RequestRecord(0L, 0, 0, 0, 0, 0, "", 0)))
      }

      val req = reqDataset.filter(r => r.timestamp > 0 && r.project_id > 0)
      val count = req.count()
      println(s"#####there $count records in req")

      if (count > 0) {
        val reqTable = tableEnv.fromDataSet(req)

        //req campaign
        println(s"#####start to process request campaign report...")
        val reqCampaignDF = reqTable.groupBy(CAM_COLS)
          .select(s"$CAM_COLS, ${makeSumString(REQCAM_COLS)}")
        val cTable = if (patch) campaignTablePathch else campaignTable
        val reqCampaignDs = tableEnv.toDataSet[RequestCampaign](reqCampaignDF)
        //reqCampaignDs.output(new MysqlOutput[RequestCampaign](REQCAM, jdbcConf, cTable))
        reqCampaignDs.output(new MysqlOut[RequestCampaign](REQCAM, jdbcConf, cTable))
        println(s"#####request campaign report processed end...")

        /*//req campaign location
        println(s"#####start to process request campaign report with location...")
        val reqCampaignDFL = reqTable.groupBy(CAML_COLS)
          .select(s"$CAML_COLS, ${makeSumString(REQCAM_COLS)}")
        val cLTable = if (patch) campaignTableLocationPatch else campaignTableLocation
        val reqCampaignLDs = tableEnv.toDataSet[RequestCampaignWithLocation](reqCampaignDFL)
        reqCampaignLDs.output(new MysqlOutput[RequestCampaignWithLocation](REQCAML, jdbcConf, cLTable))
        println(s"#####request campaign report with location processed end...")

        //req media
        println(s"#####start to process request media report...")
        val reqMediaDF = reqTable.groupBy(MED_COLS)
          .select(s"$MED_COLS, ${makeSumString(REQMED_COLS)}")
        val mTable = if (patch) mediaTablePatch else mediaTable
        val reqMediaDs = tableEnv.toDataSet[RequestMedia](reqMediaDF)
        reqMediaDs.output(new MysqlOutput[RequestMedia](REQMED, jdbcConf, mTable))
        println(s"#####request media report processed end...")

        //req media location
        println(s"#####start to process request media with location report...")
        val reqMediaLDF = reqTable.groupBy(MEDL_COLS)
          .select(s"$MEDL_COLS, ${makeSumString(REQMED_COLS)}")
        val mLTable = if (patch) mediaTableLocationPatch else mediaTableLocation
        val reqMediaLDs = tableEnv.toDataSet[RequestMediaWithLocation](reqMediaLDF)
        reqMediaLDs.output(new MysqlOutput[RequestMediaWithLocation](REQMEDL, jdbcConf, mLTable))
        println(s"#####request media report with location  processed end...")*/
      }

      /*val cTable = if (patch) campaignTablePathch else campaignTable
      val reqCampaignDs = tableEnv.toDataSet[RequestCampaign](reqCampaignDF)
      reqCampaignDs.output(new MysqlOut[RequestCampaign](REQCAM, cTable))*/

      /*val selectCols = "bids,wins"
      val cTable = if (patch) campaignTablePathch else campaignTable
      val sqlString = s"INSERT INTO $cTable ($joinCols,$selectCols) VALUES (?,?,?,?,?,?)"
      println(s"#####sqlString = $sqlString")
      val sink: JDBCAppendTableSink = JDBCAppendTableSink.builder()
        .setDrivername("com.mysql.jdbc.Driver")
        .setDBUrl(mysqlUrl)
        .setUsername(mysqlUser)
        .setPassword(mysqlPasswd)
        .setBatchSize(100)
        .setQuery(sqlString)
        .setParameterTypes(LONG_TYPE_INFO, INT_TYPE_INFO, INT_TYPE_INFO, INT_TYPE_INFO, LONG_TYPE_INFO,
          LONG_TYPE_INFO)
        .build()
      reqCampaignDF.writeToSink(sink)*/

      /*val impDataset = if (isExistHdfsPath(job.toString, IMP)) {
        println(s"#####imp has record logs in hdfs of half hour begins with $job")
        val impPath = mkString(HDFS_PREFIX, hdfsBasePath, IMP, job.toString)
        env.readTextFile(impPath).map(r => {
          val j = JSON.parseObject(r)
          TrackRecord(IMP, job, j.getInteger("projectid"), j.getInteger("cid"),
            j.getInteger("crid"), j.getInteger("mediaid"), j.getInteger("adspaceid"),
            j.getString("location"), j.getInteger("invalid"), j.getLong("income"),
            j.getLong("cost"))
        })
      } else {
        println(s"#####imp has no record logs in hdfs of half hour begins with $job, using a null dataframe to replace")
        env.fromCollection(Seq(TrackRecord(IMP, 0L, 0, 0, 0, 0, 0, "", 0, 0L, 0L)))
      }
      val imp = impDataset.filter(r => r.timestamp > 0 && r.project_id > 0)

      val clkDataset = if (isExistHdfsPath(job.toString, CLK)) {
        println(s"#####clk has record logs in hdfs of half hour begins with $job")
        val clkPath = mkString(HDFS_PREFIX, hdfsBasePath, CLK, job.toString)
        env.readTextFile(clkPath).map(r => {
          val j = JSON.parseObject(r)
          TrackRecord(CLK, job, j.getInteger("projectid"), j.getInteger("cid"),
            j.getInteger("crid"), j.getInteger("mediaid"), j.getInteger("adspaceid"),
            j.getString("location"), j.getInteger("invalid"), j.getLong("income"),
            j.getLong("cost"))
        })
      } else {
        println(s"#####clk has no record logs in hdfs of half hour begins with $job, using a null dataframe to replace")
        env.fromCollection(Seq(TrackRecord(CLK, 0L, 0, 0, 0, 0, 0, "", 0, 0L, 0L)))
      }
      val clk = clkDataset.filter(r => r.timestamp > 0 && r.project_id > 0)

      val track = imp.union(clk)
      val tCount = track.count()
      println(s"#####there $tCount records in track")
      if(tCount >0){
        val trackTable = tableEnv.fromDataSet(track)

        //track campaign
        println(s"#####start to process track campaign report...")
        val tCampaignDF = trackTable.groupBy(CAM_COLS)
          .select(s"$CAM_COLS, ${makeSumString(TRACAM_COLS)}")
        val cTable = if (patch) campaignTablePathch else campaignTable
        val tCampaignDs = tableEnv.toDataSet[TrackCampaign](tCampaignDF)
        tCampaignDs.output(new MysqlOutput[TrackCampaign](TRACAM, jdbcConf, cTable))
        println(s"#####track campaign report processed end...")

        //req campaign location
        println(s"#####start to process track campaign report with location...")
        val tCampaignDFL = trackTable.groupBy(CAML_COLS)
          .select(s"$CAML_COLS, ${makeSumString(TRACAM_COLS)}")
        val cLTable = if (patch) campaignTableLocationPatch else campaignTableLocation
        val tCampaignLDs = tableEnv.toDataSet[TrackCampaignWithLocation](tCampaignDFL)
        tCampaignLDs.output(new MysqlOutput[TrackCampaignWithLocation](TRACAML, jdbcConf, cLTable))
        println(s"#####track campaign report with location processed end...")

        //track media
        println(s"#####start to process track media report...")
        val tMediaDF = trackTable.groupBy(MED_COLS)
          .select(s"$MED_COLS, ${makeSumString(TRAMED_COLS)}")
        val mTable = if (patch) mediaTablePatch else mediaTable
        val tMediaDs = tableEnv.toDataSet[TrackMedia](tMediaDF)
        tMediaDs.output(new MysqlOutput[TrackMedia](TRAMED, jdbcConf, mTable))
        println(s"#####track media report processed end...")

        //track media location
        println(s"#####start to process track media with location report...")
        val tMediaLDF = trackTable.groupBy(MEDL_COLS)
          .select(s"$MEDL_COLS, ${makeSumString(TRAMED_COLS)}")
        val mLTable = if (patch) mediaTableLocationPatch else mediaTableLocation
        val tMediaLDs = tableEnv.toDataSet[TrackMediaWithLocation](tMediaLDF)
        tMediaLDs.output(new MysqlOutput[TrackMediaWithLocation](TRAMEDL, jdbcConf, mLTable))
        println(s"#####track media report with location  processed end...")
      }*/
    }
    env.execute(s"flink carpo")
    println(s"all jobs are finished, using time:${(System.currentTimeMillis() - startTime) / 1000} s..")
  }
}
