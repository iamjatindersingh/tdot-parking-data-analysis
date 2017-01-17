package com.sqoopdata.tdot

/**
  * @author jsingh on 2016-11-12.
  */
abstract class Item[T] {

  def getInt(n: String) : Integer = n match {
    case "" => 0
    case _ => n.toInt
  }

  def getBigInt(n: String) : BigInt = n match {
    case "" => 0
    case _ => BigInt(n)
  }

  def getBigDecimal(n: String) : BigDecimal = n match {
    case "" => 0
    case _ => BigDecimal(n)
  }

  def getLongDate(d: String) : Long = d match {
    case "" => 0
    case _ => getDateFormat.parse(d).getTime
  }

  def getDateFormat : java.text.SimpleDateFormat = {
    val sdf = new java.text.SimpleDateFormat("yyyyMMdd")
    sdf
  }

  /**
    * Parses raw data and converts it into a T object
    *
    * @param line Line in the DataSet
    * @return     T object from the DataSet
    */
  def parse(line: String) : T
}
