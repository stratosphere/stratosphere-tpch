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

/**
 * Original query:
 *
 * {{{
 * select
 *  c_count,
 *  count(*) as custdist
 * from
 * 	(select
 *    c_custkey,
 *    count(o_orderkey)
 *   from
 *    customer left outer join orders on 
 *    c_custkey = o_custkey
 *    and o_comment not like '%WORD1%:WORD2%'
 *   group by
 *    c_custkey
 *  ) as c_orders (c_custkey, c_count)
 * group by
 * 	c_count
 * order by
 * 	custdist desc,
 *  c_count desc;
 * }}}
 *
 * @param dop Degree of parallelism
 * @param inPath Base input path
 * @param outPath Output path
 * @param word1 Query parameter `WORD1`
 * @param word2 Query parameter `WORD2`
 */
class TPCHQuery13(dop: Int, inPath: String, outPath: String, word1: String, word2: String) extends TPCHQuery(13, dop, inPath, outPath) {

  // TODO: final order by not implemented yet.
  
  def plan(): ScalaPlan = {

    val commentPattern = ".*("+word1+").*("+word2+").*"
    
    // scan orders
    val orders = ( Order(inPath)
    			   filter { o => !(o.comment.matches(commentPattern)) }
                   map { o => o.custKey }
                 )
    
    // scan customers
    val customers = ( Customer(inPath)
                      map { c => c.custKey }
                    )
    
    // count orders per customer
    val custOrderCnt = ( customers cogroup orders where { c => c } isEqualTo { o => o } 
                         map { (cust, orders) => (orders.length, 1) }
                       )
    
    // group by order count and compute count histogram
    val orderCntGroup = ( custOrderCnt groupBy { oc => oc._1 } 
                          reduce { (oc1, oc2) => (oc1._1, (oc1._2 + oc2._2) ) }
                        )
    
    // write result
    val out = orderCntGroup.write(s"$outPath/query13.result", DelimitedOutputFormat(x => "%d|%d".format(x._1, x._2)))

    val plan = new ScalaPlan(Seq(out), queryName)
    plan.setDefaultParallelism(dop)

    plan
  }
}
