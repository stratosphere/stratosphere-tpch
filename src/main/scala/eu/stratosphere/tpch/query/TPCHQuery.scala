/* *********************************************************************************************************************
 *
 * Copyright (C) 2010 by the Stratosphere project (http://stratosphere.eu)
 *
 * Licensed under the Apache License, Version 2.0 (the "License"); you may not use this file except in compliance with
 * the License. You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software distributed under the License is distributed on
 * an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the License for the
 * specific language governing permissions and limitations under the License.
 *
 * ********************************************************************************************************************
 */
package eu.stratosphere.tpch.query

import eu.stratosphere.scala._
import eu.stratosphere.scala.operators._

import org.joda.time.DateTime
import scopt.OptionParser

/**
 * An abstract base class for all TPC-H queries.
 */
abstract class TPCHQuery(dop: Int, inPath: String, outPath: String) extends Serializable {

  /*~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~*
   * Abstract Methods
   *~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~*/

  def plan(): ScalaPlan
}

/*~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~*
 * Case classes for the TPC-H schema
 *~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~*/

case class Nation(
  `nationKey`: Int,
  `name`: String,
  `regionKey`: Int,
  `comment`: String)

case class Region(
  `regionKey`: Int,
  `name`: String,
  `comment`: String)

case class Part(
  `partKey`: Int,
  `name`: String,
  `mfgr`: String,
  `brand`: String,
  `partType`: String,
  `size`: Int,
  `container`: String,
  `retailPrice`: Double,
  `comment`: String)

case class Supplier(
  `suppKey`: Int,
  `name`: String,
  `address`: String,
  `nationKey`: Int,
  `phone`: String,
  `accBal`: Double,
  `comment`: String)

case class PartSupp(
  `partKey`: Int,
  `suppKey`: Int,
  `availQty`: Int,
  `supplyCost`: Double,
  `comment`: String)

case class Customer(
  `custKey`: Int,
  `name`: String,
  `address`: String,
  `nationKey`: Int,
  `phone`: String,
  `accBal`: Double,
  `mktSegment`: String,
  `comment`: String)

case class Order(
  `orderKey`: Int,
  `custKey`: Int,
  `orderStatus`: String,
  `totalPrice`: Double,
  `orderDate`: String,
  `orderPriority`: String,
  `clerk`: String,
  `shipPriority`: Int,
  `comment`: String)

case class Lineitem(
  `orderKey`: Int,
  `partKey`: Int,
  `suppKey`: Int,
  `lineNumber`: Int,
  `quantity`: Int,
  `extendedPrice`: Double,
  `discount`: Double,
  `tax`: Double,
  `returnFlag`: String,
  `lineStatus`: String,
  `shipDate`: String,
  `commitDate`: String,
  `receiptDate`: String,
  `shipInstruct`: String,
  `shipMode`: String,
  `comment`: String)

/*~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~*
 * DataSource helpers
 *~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~*/

object Nation {
  def apply(inPath: String) = DataSource(s"$inPath/nation.tbl", CsvInputFormat[Nation]("\n", "|"))
}

object Region {
  def apply(inPath: String) = DataSource(s"$inPath/region.tbl", CsvInputFormat[Region]("\n", "|"))
}

object Part {
  def apply(inPath: String) = DataSource(s"$inPath/part.tbl", CsvInputFormat[Part]("\n", "|"))
}

object Supplier {
  def apply(inPath: String) = DataSource(s"$inPath/supplier.tbl", CsvInputFormat[Supplier]("\n", "|"))
}

object PartSupp {
  def apply(inPath: String) = DataSource(s"$inPath/partsupp.tbl", CsvInputFormat[PartSupp]("\n", "|"))
}

object Customer {
  def apply(inPath: String) = DataSource(s"$inPath/customer.tbl", CsvInputFormat[Customer]("\n", "|"))
}

object Order {
  def apply(inPath: String) = DataSource(s"$inPath/order.tbl", CsvInputFormat[Order]("\n", "|"))
}

object Lineitem {
  def apply(inPath: String) = DataSource(s"$inPath/lineitem.tbl", CsvInputFormat[Lineitem]("\n", "|"))
}

/*~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~*
 * Date handling helpers
 *~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~*/

object Date {

  def fromString(dateTime: String): DateTime = DateTime.parse(dateTime)

  def toString(dateTime: DateTime): String = dateTime.toString("yyyy-MM-dd")
}

/*~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~*
 * CLI Configuration
 *~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~*/

case class TPCHConfig(
  // generic parameters
  val `queryNo`: Int = 1,
  val `dop`: Int = 1,
  val `inPath`: String = "",
  val `outPath`: String = "",
  // query specific parameters
  val `brands`: Seq[String] = Seq(),
  val `color`: String = "",
  val `container`: String = "",
  val `date`: String = "",
  val `delta`: Int = 0,
  val `discount`: Double = 0.00,
  val `fraction`: Double = 0.00,
  val `i`: Seq[Int] = Seq(),
  val `nations`: Seq[String] = Seq(),
  val `quantities`: Seq[Int] = Seq(),
  val `region`: String = "",
  val `segment`: String = "",
  val `shipmodes`: Seq[String] = Seq(),
  val `sizes`: Seq[Int] = Seq(),
  val `type`: String = "",
  val `words`: Seq[String] = Seq())

/*~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~*
 * Companion object
 *~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~*/

object TPCHQuery {

  val NL = System.getProperty("line.separator")

  def parser = new scopt.OptionParser[TPCHConfig]("stratosphere-tpch") {
    // header
    head("Stratosphere TPC-H Query Runner", "(version 1.0.0)")

    // help option
    help("help")

    cmd("Q01")
      .action { (x, c) => c.copy(queryNo = 1) }
      .children(commonQueryArgs(this): _*)
      .children(
        arg[Int]("[delta]")
          .action { (x, c) => c.copy(`delta` = x) }
          .text("Query parameter" + NL))

    cmd("Q02")
      .action { (x, c) => c.copy(queryNo = 2) }
      .children(commonQueryArgs(this): _*)
      .children(
        arg[Int]("[size]")
          .action { (x, c) => c.copy(`sizes` = c.sizes :+ x) }
          .text("Query parameter"),
        arg[String]("[type]")
          .action { (x, c) => c.copy(`type` = x) }
          .text("Query parameter"),
        arg[String]("[region]")
          .action { (x, c) => c.copy(`region` = x) }
          .text("Query parameter" + NL))

    cmd("Q03")
      .action { (x, c) => c.copy(queryNo = 3) }
      .children(commonQueryArgs(this): _*)
      .children(
        arg[String]("[segment]")
          .action { (x, c) => c.copy(`segment` = x) }
          .text("Query parameter"),
        arg[String]("[date]")
          .action { (x, c) => c.copy(`date` = x) }
          .text("Query parameter" + NL))

    cmd("Q04")
      .action { (x, c) => c.copy(queryNo = 4) }
      .children(commonQueryArgs(this): _*)
      .children(
        arg[String]("[out]")
          .action { (x, c) => c.copy(outPath = x) }
          .text("Output path for the query result"),
        arg[String]("[date]")
          .action { (x, c) => c.copy(`date` = x) }
          .text("Query parameter" + NL))

    cmd("Q05")
      .action { (x, c) => c.copy(queryNo = 5) }
      .children(commonQueryArgs(this): _*)
      .children(
        arg[String]("[region]")
          .action { (x, c) => c.copy(`region` = x) }
          .text("Query parameter"),
        arg[String]("[date]")
          .action { (x, c) => c.copy(`date` = x) }
          .text("Query parameter" + NL))

    cmd("Q06")
      .action { (x, c) => c.copy(queryNo = 6) }
      .children(commonQueryArgs(this): _*)
      .children(
        arg[String]("[date]")
          .action { (x, c) => c.copy(`date` = x) }
          .text("Query parameter"),
        arg[Double]("[discount]")
          .action { (x, c) => c.copy(`discount` = x) }
          .text("Query parameter"),
        arg[Int]("[quantity]")
          .action { (x, c) => c.copy(`quantities` = c.quantities :+ x) }
          .text("Query parameter" + NL))

    cmd("Q07")
      .action { (x, c) => c.copy(queryNo = 7) }
      .children(commonQueryArgs(this): _*)
      .children(
        arg[String]("[nation1]")
          .action { (x, c) => c.copy(`nations` = c.nations :+ x) }
          .text("Query parameter"),
        arg[String]("[nation2]")
          .action { (x, c) => c.copy(`nations` = c.nations :+ x) }
          .text("Query parameter" + NL))

    cmd("Q08")
      .action { (x, c) => c.copy(queryNo = 8) }
      .children(commonQueryArgs(this): _*)
      .children(
        arg[String]("[nation]")
          .action { (x, c) => c.copy(`nations` = c.nations :+ x) }
          .text("Query parameter"),
        arg[String]("[region]")
          .action { (x, c) => c.copy(`region` = x) }
          .text("Query parameter"),
        arg[String]("[type]")
          .action { (x, c) => c.copy(`type` = x) }
          .text("Query parameter" + NL))

    cmd("Q09")
      .action { (x, c) => c.copy(queryNo = 9) }
      .children(commonQueryArgs(this): _*)
      .children(
        arg[String]("[color]")
          .action { (x, c) => c.copy(`color` = x) }
          .text("Query parameter" + NL))

    cmd("Q10")
      .action { (x, c) => c.copy(queryNo = 10) }
      .children(commonQueryArgs(this): _*)
      .children(
        arg[String]("[date]")
          .action { (x, c) => c.copy(`date` = x) }
          .text("Query parameter" + NL))

    cmd("Q11")
      .action { (x, c) => c.copy(queryNo = 11) }
      .children(commonQueryArgs(this): _*)
      .children(
        arg[String]("[nation]")
          .action { (x, c) => c.copy(`nations` = Seq(x)) }
          .text("Query parameter"),
        arg[Double]("[fraction]")
          .action { (x, c) => c.copy(`fraction` = x) }
          .text("Query parameter" + NL))

    cmd("Q12")
      .action { (x, c) => c.copy(queryNo = 12) }
      .children(commonQueryArgs(this): _*)
      .children(
        arg[String]("[shipmode1]")
          .action { (x, c) => c.copy(`shipmodes` = c.shipmodes :+ x) }
          .text("Query parameter"),
        arg[String]("[shipmode2]")
          .action { (x, c) => c.copy(`shipmodes` = c.shipmodes :+ x) }
          .text("Query parameter"),
        arg[String]("[date]")
          .action { (x, c) => c.copy(`date` = x) }
          .text("Query parameter" + NL))

    cmd("Q13")
      .action { (x, c) => c.copy(queryNo = 13) }
      .children(commonQueryArgs(this): _*)
      .children(
        arg[String]("[word1]")
          .action { (x, c) => c.copy(`shipmodes` = c.words :+ x) }
          .text("Query parameter"),
        arg[String]("[word2]")
          .action { (x, c) => c.copy(`shipmodes` = c.words :+ x) }
          .text("Query parameter" + NL))
  }

  def commonQueryArgs(parser: scopt.OptionParser[TPCHConfig]): Seq[scopt.OptionDef[_, eu.stratosphere.tpch.query.TPCHConfig]] = Seq(
    parser.arg[Int]("[dop]")
      .action { (x, c) => c.copy(dop = x) }
      .text("Degree of parallelism"),
    parser.arg[String]("[in]")
      .action { (x, c) => c.copy(inPath = x) }
      .text("Base path for the TPC-H inputs"),
    parser.arg[String]("[out]")
      .action { (x, c) => c.copy(outPath = x) }
      .text("Output path for the query result"))
}
