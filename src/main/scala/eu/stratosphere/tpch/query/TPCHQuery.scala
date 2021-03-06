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

import eu.stratosphere.scala.ScalaPlan

import eu.stratosphere.tpch.config.TPCHConfig

import org.joda.time.DateTime

/**
 * An abstract base class for all TPC-H queries.
 */
abstract class TPCHQuery(queryNo: Int, dop: Int, inPath: String, outPath: String) extends Serializable {

  val queryName = f"TPC-H Query #${queryNo}%02d"

  /**
   * Abstract plan generation method. In concrete implementations, use the
   * parameters passed to the TPCHQuery subclass to construct a parameterized
   * ScalaPlan.
   */
  def plan(): ScalaPlan
}

object TPCHQuery {

  def string2date(dateTime: String): DateTime = DateTime.parse(dateTime)

  def date2string(dateTime: DateTime): String = dateTime.toString("yyyy-MM-dd")

  /**
   * Factory method for creation of TPC-H Queries.
   */
  def createQuery(c: TPCHConfig): Option[TPCHQuery] = c.queryNo match {
    case 1  => Option(new TPCHQuery01(c.dop, c.inPath, c.outPath, c.delta))
    case 2  => Option(new TPCHQuery02(c.dop, c.inPath, c.outPath, c.sizes(0), c.ptype, c.region))
    case 3  => Option(new TPCHQuery03(c.dop, c.inPath, c.outPath, c.segment, string2date(c.date)))
    case 4  => Option(new TPCHQuery04(c.dop, c.inPath, c.outPath, string2date(c.date)))
    case 5  => Option(new TPCHQuery05(c.dop, c.inPath, c.outPath, c.region, string2date(c.date)))
    case 6  => Option(new TPCHQuery06(c.dop, c.inPath, c.outPath, string2date(c.date), c.discount, c.quantities(0)))
    case 7  => Option(new TPCHQuery07(c.dop, c.inPath, c.outPath, c.nations(0), c.nations(1)))
    case 8  => Option(new TPCHQuery08(c.dop, c.inPath, c.outPath, c.nations(0), c.region, c.ptype))
    case 9  => Option(new TPCHQuery09(c.dop, c.inPath, c.outPath, c.color))
    case 10 => Option(new TPCHQuery10(c.dop, c.inPath, c.outPath, string2date(c.date)))
    case 11 => Option(new TPCHQuery11(c.dop, c.inPath, c.outPath, c.nations(0), c.fraction))
    case 12 => Option(new TPCHQuery12(c.dop, c.inPath, c.outPath, c.shipmodes(0), c.shipmodes(1), string2date(c.date)))
    case 13 => Option(new TPCHQuery13(c.dop, c.inPath, c.outPath, c.words(0), c.words(1)))
    case 14 => Option(new TPCHQuery14(c.dop, c.inPath, c.outPath, string2date(c.date)))
    case 18 => Option(new TPCHQuery18(c.dop, c.inPath, c.outPath, c.quantities(0)))
    case 19 => Option(new TPCHQuery19(c.dop, c.inPath, c.outPath, c.brands(0), c.quantities(0), c.brands(1), c.quantities(1), c.brands(2), c.quantities(2)))
    case _  => Option(null)
  }
}
