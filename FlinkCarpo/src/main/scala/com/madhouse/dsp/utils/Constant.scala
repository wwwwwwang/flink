package com.madhouse.dsp.utils

object Constant {
  val REQCAM = "RequestCampaign"
  val REQCAML = "RequestCampaignWithLocation"
  val REQMED = "RequestMedia"
  val REQMEDL = "RequestMediaWithLocation"
  val TRACAM = "TrackCampaign"
  val TRACAML = "TrackCampaignWithLocation"
  val TRAMED = "TrackMedia"
  val TRAMEDL = "TrackMediaWithLocation"

  val CAM_COLS = "timestamp,project_id,campaign_id,material_id"
  val CAML_COLS = "timestamp,project_id,campaign_id,material_id,location"
  val REQCAM_COLS = "bids,wins"
  val TRACAM_COLS = "imps,clks,vimps,vclks,cost"

  val MED_COLS = "timestamp,media_id,adspace_id"
  val MEDL_COLS = "timestamp,media_id,adspace_id,location"
  val REQMED_COLS = "reqs,bids,wins,errs"
  val TRAMED_COLS = "imps,clks,vimps,vclks,income"

  val HDFS_PREFIX = "hdfs://"
  val REQ = "req"
  val IMP = "imp"
  val CLK = "clk"
}
