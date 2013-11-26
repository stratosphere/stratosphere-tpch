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

import scala.language.reflectiveCalls

import eu.stratosphere.scala._
import eu.stratosphere.scala.operators._

import eu.stratosphere.tpch.schema._

/**
 * Original query:
 *
 * {{{
 * select
 * 	l_returnflag,
 * 	l_linestatus,
 * 	sum(l_quantity) as sum_qty,
 * 	sum(l_extendedprice) as sum_base_price,
 * 	sum(l_extendedprice * (1 - l_discount)) as sum_disc_price,
 * 	sum(l_extendedprice * (1 - l_discount) * (1 + l_tax)) as sum_charge,
 * 	avg(l_quantity) as avg_qty,
 * 	avg(l_extendedprice) as avg_price,
 * 	avg(l_discount) as avg_disc,
 * 	count(*) as count_order
 * from
 * 	lineitem
 * where
 * 	l_shipdate <= date '1998-12-01' - interval ':DELTA' day (3)
 * group by
 * 	l_returnflag,
 * 	l_linestatus
 * order by
 * 	l_returnflag,
 * 	l_linestatus;
 * }}}
 *
 * @param dop Degree of parallism
 * @param inPath Base input path
 * @param outPath Output path
 * @param delta Query parameter `DELTA`
 */
class TPCHQuery01(dop: Int, inPath: String, outPath: String, delta: Int) extends TPCHQuery(1, dop, inPath, outPath) {

  case class Aggregate(
    returnFlag: String,
    lineStatus: String,
    sumQty: Int,
    sumBasePrice: Double,
    sumDiscPrice: Double,
    sumCharge: Double,
    sumDiscount: Double,
    countOrder: Int)

  def plan(): ScalaPlan = {

    val dateMax = TPCHQuery.string2date("1998-12-01").minusDays(delta)

    val expression = Lineitem(inPath)
      .filter(l => TPCHQuery.string2date(l.shipDate).compareTo(dateMax) <= 0)
      .map(l => Aggregate(
        l.returnFlag,
        l.lineStatus,
        l.quantity,
        l.extendedPrice,
        l.extendedPrice * (1 - l.discount),
        l.extendedPrice * (1 - l.discount) * (1 + l.tax),
        l.discount,
        1))
      .groupBy(l => (l.returnFlag, l.lineStatus))
      .reduce((agg1, agg2) => Aggregate(
        agg1.returnFlag,
        agg1.lineStatus,
        agg1.sumQty + agg2.sumQty,
        agg1.sumBasePrice + agg2.sumBasePrice,
        agg1.sumDiscPrice + agg2.sumDiscPrice,
        agg1.sumCharge + agg2.sumCharge,
        agg1.sumDiscount + agg2.sumDiscount,
        agg1.countOrder + agg2.countOrder))
      .write(s"$outPath/query01.result", DelimitedOutputFormat(agg => "%s|%s|%d|%f|%f|%f|%f|%f|%f|%d".format(
        agg.returnFlag,
        agg.lineStatus,
        agg.sumQty,
        agg.sumBasePrice,
        agg.sumDiscPrice,
        agg.sumCharge,
        agg.sumQty / agg.countOrder.toDouble,
        agg.sumBasePrice / agg.countOrder.toDouble,
        agg.sumDiscount / agg.countOrder.toDouble,
        agg.countOrder)))
    // TODO: sort expression on (_1 asc, _2 asc)

    val plan = new ScalaPlan(Seq(expression), queryName)
    plan.setDefaultParallelism(dop)

    plan
  }
}