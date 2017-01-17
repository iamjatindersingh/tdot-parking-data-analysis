package com.sqoopdata.tdot

/**
  * @author jsingh on 2016-11-12.
  */
case class Ticket(year: Int,                        // Year
                  tagNumberMasked: String,          // Tag Number (Masked)
                  dateOfInfraction: Long,           // Date of Infraction
                  infractionCode: Int,              // Infraction Code
                  infractionDescription: String,    // Infraction Code Description
                  fineAmount: BigDecimal,           // Fine Amount
                  timeOfInfraction: String,         // Time of Infraction
                  location1: String,
                  location2: String,                // Address of Infraction Point
                  location3: String,
                  location4:String,
                  province: String)                 // Province

object TorontoTicket extends Item[Ticket] {


  override def parse(line: String): Ticket = {
    val data = line.split(",", -1) // We do not want to skip trailing characters

    Ticket(
      getInt(data(0)),
      data(1),
      getLongDate(data(2)),
      getInt(data(3)),
      data(4),
      getBigDecimal(data(5)),
      data(6),
      data(7),
      data(8),
      data(9),
      data(10),
      data(11))
  }
}
