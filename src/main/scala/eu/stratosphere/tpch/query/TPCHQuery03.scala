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
import org.joda.time.DateTime

/**
 * Original query:
 *
 * {{{
 * select
 * 	l_orderkey,
 * 	sum(l_extendedprice * (1 - l_discount)) as revenue,
 * 	o_orderdate,
 * 	o_shippriority
 * from
 * 	customer,
 * 	orders,
 * 	lineitem
 * where
 * 	c_mktsegment = ':SEGMENT'
 * 	and c_custkey = o_custkey
 * 	and l_orderkey = o_orderkey
 * 	and o_orderdate < date ':DATE'
 * 	and l_shipdate > date ':DATE'
 * group by
 * 	l_orderkey,
 * 	o_orderdate,
 * 	o_shippriority
 * order by
 * 	revenue desc,
 * 	o_orderdate;
 * }}}
 *
 * @param dop Degree of parallism
 * @param inPath Base input path
 * @param outPath Output path
 * @param segment Query parameter `SEGMENT`
 * @param date Query parameter `DATE`
 */
class TPCHQuery03(dop: Int, inPath: String, outPath: String, segment: String, date: DateTime) extends TPCHQuery(3, dop, inPath, outPath) {

  def plan(): ScalaPlan = {

    val customer = Customer(inPath) filter { c => c.mktSegment == segment }
    val order = Order(inPath) filter { x => TPCHQuery.string2date(x.orderDate).compareTo(date) < 0 }
    val lineitem = Lineitem(inPath) filter { x => TPCHQuery.string2date(x.shipDate).compareTo(date) > 0 }

    val e1 = customer join order where { _.custKey } isEqualTo { _.orderKey } map {
      (c, o) => (o.orderKey, o.orderDate, o.orderPriority)
    }
    val e2 = e1 join lineitem where { _._1 } isEqualTo { _.orderKey } map {
      (x, l) => (x._1, l.extendedPrice * (1 - l.discount), x._2, x._3)
    } groupBy(_._1) reduce {
      (x, y) => (x._1, x._2 + y._2, x._3, x._4)
    }
    // TODO: sort e4 on (_2 desc, _3 asc)

    val expression = e2.write(s"$outPath/query03.result", DelimitedOutputFormat(x => "%d|%f|%s|%d".format(x._1, x._2, x._2, x._4)))

    val plan = new ScalaPlan(Seq(expression), queryName)
    plan.setDefaultParallelism(dop)

    plan
  }
}