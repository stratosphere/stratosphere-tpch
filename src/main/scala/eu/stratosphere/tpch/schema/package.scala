package eu.stratosphere.tpch

import eu.stratosphere.scala.DataSource
import eu.stratosphere.scala.operators.CsvInputFormat

package object schema {

  /*~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~*
   * Case classes for the TPC-H schema
   *~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~*/

  case class Nation(
    nationKey: Int,
    name: String,
    regionKey: Int,
    comment: String)

  case class Region(
    regionKey: Int,
    name: String,
    comment: String)

  case class Part(
    partKey: Int,
    name: String,
    mfgr: String,
    brand: String,
    ptype: String,
    size: Int,
    container: String,
    retailPrice: Double,
    comment: String)

  case class Supplier(
    suppKey: Int,
    name: String,
    address: String,
    nationKey: Int,
    phone: String,
    accBal: Double,
    comment: String)

  case class PartSupp(
    partKey: Int,
    suppKey: Int,
    availQty: Int,
    supplyCost: Double,
    comment: String)

  case class Customer(
    custKey: Int,
    name: String,
    address: String,
    nationKey: Int,
    phone: String,
    accBal: Double,
    mktSegment: String,
    comment: String)

  case class Order(
    orderKey: Int,
    custKey: Int,
    orderStatus: String,
    totalPrice: Double,
    orderDate: String,
    orderPriority: String,
    clerk: String,
    shipPriority: Int,
    comment: String)

  case class Lineitem(
    orderKey: Int,
    partKey: Int,
    suppKey: Int,
    lineNumber: Int,
    quantity: Int,
    extendedPrice: Double,
    discount: Double,
    tax: Double,
    returnFlag: String,
    lineStatus: String,
    shipDate: String,
    commitDate: String,
    receiptDate: String,
    shipInstruct: String,
    shipMode: String,
    comment: String)

  /*~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~*
   * DataSource helpers
   *~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~*/

  object Nation {
    def apply(inPath: String) = DataSource(s"$inPath/nation.tbl", CsvInputFormat[Nation]("\n", '|'))
  }

  object Region {
    def apply(inPath: String) = DataSource(s"$inPath/region.tbl", CsvInputFormat[Region]("\n", '|'))
  }

  object Part {
    def apply(inPath: String) = DataSource(s"$inPath/part.tbl", CsvInputFormat[Part]("\n", '|'))
  }

  object Supplier {
    def apply(inPath: String) = DataSource(s"$inPath/supplier.tbl", CsvInputFormat[Supplier]("\n", '|'))
  }

  object PartSupp {
    def apply(inPath: String) = DataSource(s"$inPath/partsupp.tbl", CsvInputFormat[PartSupp]("\n", '|'))
  }

  object Customer {
    def apply(inPath: String) = DataSource(s"$inPath/customer.tbl", CsvInputFormat[Customer]("\n", '|'))
  }

  object Order {
    def apply(inPath: String) = DataSource(s"$inPath/order.tbl", CsvInputFormat[Order]("\n", '|'))
  }

  object Lineitem {
    def apply(inPath: String) = DataSource(s"$inPath/lineitem.tbl", CsvInputFormat[Lineitem]("\n", '|'))
  }
}