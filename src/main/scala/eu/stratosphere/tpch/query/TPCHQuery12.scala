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
import eu.stratosphere.pact.client.LocalExecutor
import eu.stratosphere.scala._
import eu.stratosphere.scala.operators._
import eu.stratosphere.tpch.schema._
import org.joda.time.DateTime

/**
 * Original query:
 *
 * {{{
 * select
 *  l_shipmode,
 *  sum(case when o_orderpriority = '1-URGENT'
 *                or o_orderpriority = '2-HIGH'
 *           then 1
 *           else 0
 *      end) as high_line_count,
 *  sum(case when o_orderpriority <> '1-URGENT'
 *                and o_orderpriority <> '2-HIGH'
 *           then 1
 *           else 0
 *      end) as low_line_count
 * from
 *  orders,
 *  lineitem
 * where
 *  o_orderkey = l_orderkey
 *  and l_shipmode in ('SHIPMODE1', 'SHIPMODE2')
 *  and l_commitdate < l_receiptdate
 *  and l_shipdate < l_commitdate
 *  and l_receiptdate >= date 'DATE'
 *  and l_receiptdate < date 'DATE' + interval '1' year
 * group by
 *  l_shipmode
 * order by
 *  l_shipmode;
 * }}}
 *
 * @param dop Degree of parallelism
 * @param inPath Base input path
 * @param outPath Output path
 * @param shipMode1 Query parameter `SHIPMODE1`
 * @param shipMode2 Query parameter `SHIPMODE2`
 * @param date Query parameter `DATE`
 */
class TPCHQuery12(dop: Int, inPath: String, outPath: String, shipMode1: String, shipMode2: String, date: DateTime) extends TPCHQuery(12, dop, inPath, outPath) {

  // TODO: final order by not implemented yet.
  
  def plan(): ScalaPlan = {

    // compute date bounds
    val dateLowBound  = date
    val dateHighBound = date.plusYears(1)
    
    // scan orders and project
    val orders = ( Order(inPath)
                   map { o => ( if (o.orderPriority.equals("1-URGENT") || o.orderPriority.equals("2-HIGH"))
            		   				(o.orderKey, 1, 0) 
            		   			 else
            		   			    (o.orderKey, 0, 1)
            		   		   ) 
                       }
                 )
    
    // scan lineitem, filter, and project
    val lineitem = ( Lineitem(inPath)
    			     filter { l => ( l.commitDate.compareTo(l.receiptDate) < 0
    			                     && l.shipDate.compareTo(l.commitDate) < 0
    			                     && TPCHQuery.string2date(l.receiptDate).compareTo(dateLowBound) >= 0
    			                     && TPCHQuery.string2date(l.receiptDate).compareTo(dateHighBound) < 0
    			                     && ( l.shipMode.equals(shipMode1) || l.shipMode.equals(shipMode2) )
    			                   ) 
    			            }
                     map { l => (l.orderKey, l.shipMode) }
                   )
    
    // join, group, and aggregate counts
    val ol = ( orders join lineitem where { o => o._1 } isEqualTo { l => l._1 }
               map { (o, l) => (l._2, o._2, o._3) }
               groupBy { x => x._1 }
               reduce { (x1, x2) => (x1._1, (x1._2 + x2._2), (x1._3 + x2._3) ) }
             )
   
    // write result
    val out = ol.write(s"$outPath/query12.result", DelimitedOutputFormat(x => "%s|%d|%d".format(x._1, x._2, x._3)))

    val plan = new ScalaPlan(Seq(out), queryName)
    plan.setDefaultParallelism(dop)

    plan
  }
}

object RunWordCount {
  def main(args: Array[String]) {
    val q = new TPCHQuery12(4, "file:///home/fhueske/tpch-s1/text", "file:///home/fhueske/result", "MAIL", "SHIP", new DateTime(1994,1,1,0,0))
    LocalExecutor.execute(q.plan)
  }
}
