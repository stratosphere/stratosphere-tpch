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
abstract class TPCHQuery(queryNo: Int, dop: Int, inPath: String, outPath: String) extends Serializable {

  val queryName = s"TPC-H Query #${queryNo}02d"

  /**
   * Abstract plan generation method. In concrete implementations, use the
   * parameters passed to the TPCHQuery subclass to construct a parameterized
   * ScalaPlan.
   */
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
