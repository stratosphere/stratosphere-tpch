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
class TPCHQuery04(dop: Int, inPath: String, outPath: String, date: DateTime) extends TPCHQuery(4, dop, inPath, outPath) {

  // TODO: orderBy/Sort not implemented
  
  def plan(): ScalaPlan = {

    val dateMin = date
    val dateMax = date.plusMonths(3)

    // scan lineitem, filter, project, and compute distinct orders
    val lineitem = ( Lineitem(inPath) 
                     filter (l => TPCHQuery.string2date(l.commitDate).compareTo(TPCHQuery.string2date(l.receiptDate)) < 0 )
                     map { l => l.orderKey }
                     groupBy( l => l ) 
                     reduce { (x, y) => x }
                   )
    
    // scan orders, filter, and project
    val orders = ( Order(inPath) 
                   filter (o => TPCHQuery.string2date(o.orderDate).compareTo(dateMin) >= 0 && TPCHQuery.string2date(o.orderDate).compareTo(dateMax) < 0)
                   map (o => (o.orderKey, o.orderPriority))
                 )

    // join orders and lineitems, group by order priority, and count
    val lo = ( orders join lineitem where ( o => o._1) isEqualTo ( l => l) 
               map { (o, l) => (o._2, 1) } 
               groupBy(_._1) 
               reduce { (o1, o2) => (o1._1, (o1._2 + o2._2) ) }
             )

    // write result
    val expression = lo.write(s"$outPath/query04.result", DelimitedOutputFormat(x => "%s|%d".format(x._1, x._2)))

    val plan = new ScalaPlan(Seq(expression), queryName)
    plan.setDefaultParallelism(dop)

    plan
  }
}
