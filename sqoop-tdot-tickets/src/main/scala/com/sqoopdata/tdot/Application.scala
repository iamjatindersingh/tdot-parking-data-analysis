package com.sqoopdata.tdot

import org.apache.spark.{SparkConf, SparkContext}

/**
  * Main entry point to Sqoop-ing TDOT Ticketing Data.
  *
  * @author jsingh on 2016-11-12.
  */
object Application {

  def main(args: Array[String]): Unit = {

    if(args == null) return

    // Step 1. Initialize Spark Context
    val sparkConf = new SparkConf().setAppName("SqoopTDOTTickets")

    // Use all cores available on the PC
    sparkConf.setMaster("local[*]")

    val sparkContext = new SparkContext(sparkConf)

    // Step 2. Load Data
    val torontoRawTicketingData = sparkContext.textFile(args(0))

    // Step 3. Parse Data
    val torontoTickets = torontoRawTicketingData.filter(line => !isHeader(line)).map(TorontoTicket.parse)

    // Step 4. Cache Data
    //
    //  NOTE: On local computer, this operation will fail due to amount of data.
    //        If you are running on a cluster, you must cache for better performance.
    //torontoTickets.cache()

    // Initialize Currency Formatter
    val currencyFormatter = java.text.NumberFormat.getCurrencyInstance
    val numberFormatter = java.text.NumberFormat.getNumberInstance

    println("\n\n** Data Analysis **")

    // Get total tickets by year
    val totalTicketsIssuedByYear = torontoTickets.map(ticket => (ticket.year, 1)).reduceByKey(_ + _)

    // Add all tickets and get a sum
    val totalTicketsIssuedToDate = totalTicketsIssuedByYear.map(_._2).reduce(_ + _)

    println("\n\nTotal # of tickets issued between 2008 and 2015 are ~" + numberFormatter.format(totalTicketsIssuedToDate))
    totalTicketsIssuedByYear.map(ticket => (ticket._2, ticket._1)).sortByKey(ascending = false).foreach(item => {
      val yearTotal = numberFormatter.format(item._1)
      val year = item._2
      println(s"For year $year, City issued $yearTotal tickets.")
    })

    val totalInfractionAmountByYear = torontoTickets.map(ticket => (ticket.year, ticket.fineAmount)).reduceByKey(_ + _)
    val totalInfractionAmountToDate = currencyFormatter.format(totalInfractionAmountByYear.map(_._2).reduce(_ + _))

    println(s"\n\nTotal infraction amount to date stands at ~$totalInfractionAmountToDate")
    totalInfractionAmountByYear.map(ticket => (ticket._2, ticket._1)).sortByKey(ascending = false).foreach(item => {
      val totalYearInfractionAmount = currencyFormatter.format(item._1)
      val year = item._2
      println(s"For year $year, City made ~$totalYearInfractionAmount")
    })


    val totalTicketsIssuedByLocation = torontoTickets.map(ticket => (ticket.location2, 1)).reduceByKey(_ + _)
    val top10WorstStreetsToParkInToronto =totalTicketsIssuedByLocation.map(ticket => (ticket._2, ticket._1)).sortByKey(ascending = false)

    println("\n\n")
    top10WorstStreetsToParkInToronto.take(10).foreach(item => {
      val totalByStreet = numberFormatter.format(item._1)
      val location = item._2
      println(s"For '$location', City issued $totalByStreet tickets.")
    })

    val totalTicketsIssuedByInfractionCode = torontoTickets.map(ticket => (ticket.infractionDescription, 1)).reduceByKey(_ + _)
    val top10InfractionCodesByNumberOfTickets = totalTicketsIssuedByInfractionCode.map(ticket => (ticket._2, ticket._1)).sortByKey(ascending = false).take(10)

    println("\n\n")
    top10InfractionCodesByNumberOfTickets.foreach(item => {
      val totalTickets = numberFormatter.format(item._1)
      val infractionDescription = item._2
      println(s"For infraction '$infractionDescription', City issued $totalTickets tickets.")
    })

    // Year-Over-Year Change Analysis (in %)

    println("\n\n")
    val totalTicketsIssuedByYearMap = totalInfractionAmountByYear.collectAsMap()
    for(thisYear <- 2009 to 2015) {
      val thisYearCount = totalTicketsIssuedByYearMap(thisYear)
      val previousYearCount = totalTicketsIssuedByYearMap(thisYear - 1)
      val percentChange = ((thisYearCount - previousYearCount).toFloat / previousYearCount) * 100
      println(f"For year $thisYear, the number of tickets changed by $percentChange%.2f%% as compared to last year.")
    }

    println("\n\n")
    val totalInfractionAmountByYearMap = totalInfractionAmountByYear.collectAsMap()
    for(thisYear <- 2009 to 2015) {
      val thisYearTotalAmount = totalInfractionAmountByYearMap(thisYear)
      val previousYearTotalAmount = totalInfractionAmountByYearMap(thisYear - 1)
      val percentChange = ((thisYearTotalAmount - previousYearTotalAmount).toFloat / previousYearTotalAmount) * 100
      println(f"For year $thisYear, total infraction amount changed by $percentChange%.2f%% as compared to last year.")
    }
  }

  /**
    * Checks if a LINE is a Header
    *
    * @param line   Refers to each line in DataSet
    * @return       Result of contains
    */
  def isHeader(line: String) : Boolean = {
    line.contains("date_of_infraction")
  }
}
