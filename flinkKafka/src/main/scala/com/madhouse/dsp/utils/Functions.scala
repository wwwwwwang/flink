package com.madhouse.dsp.utils

/**
  * Created by Madhouse on 2017/12/27.
  */
object Functions extends Serializable {
  def timeprocess(time: Long, cell: Long): Long = {
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
}
