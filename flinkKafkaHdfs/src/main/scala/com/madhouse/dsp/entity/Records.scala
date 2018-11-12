package com.madhouse.dsp.entity

import org.apache.avro.generic.GenericRecord

/**
  * Created by Madhouse on 2017/12/25.
  */
case class Record(key: Long, value: String)
case class R(key:Long, value: GenericRecord)