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
 * 	o_orderpriority,
 * 	count(*) as order_count 
 * from
 * 	orders 
 * where
 *  o_orderdate >= date '[DATE]'
 *  and o_orderdate < date '[DATE]' + interval '3' month 
 *  and exists (
 *  	select *
 *   	from
 *    		lineitem
 *      where
 *      	l_orderkey = o_orderkey
 *       	and l_commitdate < l_receiptdate
 *  ) 
 * group by
 * 	o_orderpriority 
 * order by
 * 	o_orderpriority;
 * }}}
 *
 * @param dop Degree of parallism
 * @param inPath Base input path
 * @param outPath Output path
 * @param date Query parameter `DATE`
 */
class TPCHQuery04(dop: Int, inPath: String, outPath: String, date: DateTime) extends TPCHQuery(1, dop, inPath, outPath) {

  
  def plan(): ScalaPlan = {

    val dateMax = date.plusMonths(3)
    val dateMin = date
    val lineitem = Lineitem(inPath) filter (l => TPCHQuery.string2date(l.commitDate).compareTo(TPCHQuery.string2date(l.receiptDate)) < 0 ) groupBy(_.orderKey) reduce {
      (x, y) => x
    }
    val orders = Order(inPath) filter (o => TPCHQuery.string2date(o.orderDate).compareTo(dateMin) >= 0 && TPCHQuery.string2date(o.orderDate).compareTo(dateMax) < 0)
    val e1 = orders join lineitem where (_.orderKey) isEqualTo (_.orderKey) map {
      (o, l) => (o.orderPriority, 1)
    } groupBy(_._1) reduce {
      (agg1, agg2) => (agg1._1, agg1._2 + agg2._2)
    } 
    
    
    val expression = e1.write(s"$outPath/query04.result", DelimitedOutputFormat(x => "%s|%d".format(x._1, x._2)))
    
    // TODO: sort expression on (_1 asc)

    val plan = new ScalaPlan(Seq(expression), queryName)
    plan.setDefaultParallelism(dop)

    plan
  }
}