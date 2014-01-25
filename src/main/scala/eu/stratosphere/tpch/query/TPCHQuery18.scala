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

import eu.stratosphere.pact.client.LocalExecutor

/**
 * Original query:
 *
 * {{{
 * select 
 *  c_name,
 *  c_custkey, 
 *  o_orderkey,
 *  o_orderdate,
 *  o_totalprice,
 *  sum(l_quantity)
 * from 
 *  customer,
 *  orders,
 *  lineitem
 * where 
 *  o_orderkey in ( select
 *                   l_orderkey
 *                  from
 *                   lineitem
 *                  group by
 *                   l_orderkey 
 *                  having
 *                   sum(l_quantity) > [:QUANTITY]
 *                )
 *  and c_custkey = o_custkey
 *  and o_orderkey = l_orderkey
 * group by 
 *  c_name, 
 *  c_custkey, 
 *  o_orderkey, 
 *  o_orderdate, 
 *  o_totalprice
 * order by 
 *  o_totalprice desc,
 *  o_orderdate;
 * }}}
 *
 * @param dop Degree of parallelism
 * @param inPath Base input path
 * @param outPath Output path
 * @param quantity Query parameter `QUANTITY` -> default: 300
 */
class TPCHQuery18(dop: Int, inPath: String, outPath: String, quantity: Int) extends TPCHQuery(18, dop, inPath, outPath) {

  // TODO: sort, limit to 100
  
  def plan(): ScalaPlan = {

    // scan customer and project
    val customer = ( Customer(inPath)
                     map { c => (c.custKey, c.name) }
                   )

    // scan order and project
    val order = ( Order(inPath)
                  map { o => (o.orderKey, o.orderDate, o.totalPrice, o.custKey) }
                )

    // scan lineitem, group by orderKey, and compute sum
    val lineitem = ( Lineitem(inPath) 
                     map { l => (l.orderKey, l.quantity) }
                     groupBy (_._1) 
                     reduce { (x, y) => (x._1, x._2 + y._2) } 
                     filter ( _._2 > quantity )
                   )
    
    // join lineitem and orders
    val lo = ( order join lineitem where { _._1 } isEqualTo { _._1 } 
               map { (o, l) => (o._1, o._2, o._3, o._4, l._2) } 
             )
    
    // join with customer
    val loc = ( customer join lo where { _._1 } isEqualTo { _._4 } 
               map { (c, o) => (c._2, c._1, o._1, o._2, o._3, o._5) }
             )
             
    // result
    val expression = loc.write(s"$outPath/query18.result", DelimitedOutputFormat(x => "%s|%d|%d|%s|%f|%d".format(x._1, x._2, x._3, x._4, x._5, x._6)))
    
    val plan = new ScalaPlan(Seq(expression), queryName)
    plan.setDefaultParallelism(dop)

    plan
  }
}

object RunQuery {
  def main(args: Array[String]) {
    val q = new TPCHQuery18(4, "file:///home/fhueske/tpch-s1/text", "file:///home/fhueske/result", 300)
    LocalExecutor.execute(q.plan)
  }
}