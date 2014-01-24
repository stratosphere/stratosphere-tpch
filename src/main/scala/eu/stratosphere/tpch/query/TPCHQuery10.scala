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
 *  c_custkey,
 *  c_name,
 *  sum(l_extendedprice * (1 - l_discount)) as revenue,
 *  c_acctbal,
 *  n_name,
 *  c_address,
 *  c_phone,
 *  c_comment
 * from
 *  customer,
 *  orders,
 *  lineitem,
 *  nation
 * where
 *  c_custkey = o_custkey
 *  and l_orderkey = o_orderkey
 *  and o_orderdate >= date ':DATE'
 *  and o_orderdate < date ':DATE' + interval '3' month
 *  and l_returnflag = 'R'
 *  and c_nationkey = n_nationkey
 * group by
 *  c_custkey,
 *  c_name,
 *  c_acctbal,
 *  c_phone,
 *  n_name,
 *  c_address,
 *  c_comment
 * order by
 *  revenue desc;
 * }}}
 *
 * @param dop Degree of parallelism
 * @param inPath Base input path
 * @param outPath Output path
 * @param date Query parameter `DATE`
 */
class TPCHQuery10(dop: Int, inPath: String, outPath: String, date: DateTime) extends TPCHQuery(10, dop, inPath, outPath) {

  // TODO: final order-by not implemented yet.
  
  def plan(): ScalaPlan = {

    // compute date bounds
    val dateLowBound  = date
    val dateHighBound = date.plusMonths(3)
    
    // scan orders and project
    val orders = ( Order(inPath)
                   filter { o => (TPCHQuery.string2date(o.orderDate).compareTo(dateLowBound) >= 0 
                                  && TPCHQuery.string2date(o.orderDate).compareTo(dateHighBound) < 0) }
                   map { o => (o.orderKey, o.custKey) }
                 )

    // scan lineitem
    val lineitem = ( Lineitem(inPath)
                     filter { l => ( l.returnFlag.equals("R")) }
    				 map { l => (l.orderKey, (l.extendedPrice * (1.0 - l.discount))) }
                   )
    
    // scan customers
    val customers = ( Customer(inPath)
                      map { c => (c.custKey, c.name, c.accBal, c.nationKey, c.address, c.phone, c.comment) }
                    )
                    
    // scan nation and project
    val nations = ( Nation(inPath)
    			   map { n => (n.nationKey, n.name) }
                  )

    // join orders and lineitem, group by custKey, and revenue sum
    val lo = ( orders join lineitem where { o => o._1 } isEqualTo { l => l._1 }
               map { (o, l) => (o._2, l._2) } 
               groupBy { lo => lo._1 }
               reduce { (lo1, lo2) => (lo1._1, (lo1._2 + lo2._2)) }
             )

    // join orders, lineitem, and customer
    val loc = ( lo join customers where { lo => lo._1 } isEqualTo { c => c._1 }
                map { (lo, c) => (c._1, c._2, lo._2, c._3, c._4, c._5, c._6, c._7) }
              )
    
    // join orders, lineitem, customer, and nation
    val locn = ( loc join nations where { loc => loc._5 } isEqualTo { n => n._1 } 
                 map { (loc, n) => (loc._1, loc._2, loc._3, loc._4, n._2, loc._6, loc._7, loc._8) }
               )
      
    // write result
    val out = locn.write(s"$outPath/query10.result", 
                           DelimitedOutputFormat(x => "%d|%s|%f|%f|%s|%s|%s|%s".format(x._1, x._2, x._3, x._4, x._5, x._6, x._7, x._8)))

    val plan = new ScalaPlan(Seq(out), queryName)
    plan.setDefaultParallelism(dop)

    plan
  }
}
