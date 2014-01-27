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
 * 	n_name,
 *  sum(l_extendedprice * (1 - l_discount)) as revenue
 * from
 * 	customer,
 *  orders,
 *  lineitem,
 *  supplier,
 * 	nation,
 *  region
 * where
 * 	c_custkey = o_custkey
 *  and l_orderkey = o_orderkey
 *  and l_suppkey = s_suppkey
 *  and c_nationkey = s_nationkey
 *  and s_nationkey = n_nationkey
 *  and n_regionkey = r_regionkey
 *  and r_name = ':REGION'
 *  and o_orderdate >= date ':DATE'
 *  and o_orderdate < date ':DATE' + interval '1' year
 * group by
 * 	n_name
 * order by
 *  revenue desc;
 * }}}
 *
 * @param dop Degree of parallelism
 * @param inPath Base input path
 * @param outPath Output path
 * @param rName Query parameter `REGION`
 * @param date Query parameter `DATE`
 */
class TPCHQuery05(dop: Int, inPath: String, outPath: String, rName: String, date: DateTime) extends TPCHQuery(5, dop, inPath, outPath) {

  def plan(): ScalaPlan = {
    
    val minDate = date
    val maxDate = date.plusYears(1)

    // read, filter and project base relations
    val customers = ( Customer(inPath)
                      map { c => (c.custKey, c.nationKey) }
                   )

    val orders = ( Order(inPath) 
                   filter { o => TPCHQuery.string2date(o.orderDate).compareTo(minDate) >= 0 &&
                                 TPCHQuery.string2date(o.orderDate).compareTo(maxDate) < 0 }
    			   map { o => (o.orderKey, o.custKey) }
                 )
                
    val lineitems = ( Lineitem(inPath) 
                      map { l => (l.orderKey, l.suppKey, (l.extendedPrice * (1.0 - l.discount))) }
                   )
                   
    val suppliers = ( Supplier(inPath)
                      map { s => (s.suppKey, s.nationKey) }
                   )
                   
    val nations = ( Nation(inPath)
                    map { n => (n.nationKey, n.regionKey, n.name) }
                 )
                 
    val region = ( Region(inPath)
                   filter { r => (r.name.equals(rName)) }
                   map { r => r.regionKey }
                 )
              
    // join base relations and compute sum
    val result = ( region join nations where { r => r } isEqualTo { n => n._2 } 
                   map { (r, n) => (n._1, n._3) }
                          join suppliers where { n => n._1 } isEqualTo { s => s._2 }
                   map { (n, s) => (s._1, s._2, n._2) }
                          join lineitems where { s => s._1 } isEqualTo { l => l._2 }
                   map { (s, l) => (l._1, s._2, s._3, l._3 ) }
                   		  join orders where { l => l._1 } isEqualTo { o => o._1 }
                   map { (l, o) => (o._2, l._2, l._3, l._4)}
                          join customers where { l => (l._1, l._2) } isEqualTo { c => (c._1, c._2) }
                   map { (l, c) => (l._3, l._4) }
                   groupBy { l => l._1 }
                   reduce { (l1, l2) => (l1._1, (l1._2 + l2._2) ) }
                 )
       
    // write result
    val expression = result.write(s"$outPath/query05.result", DelimitedOutputFormat(x => "%s|%f".format(x._1, x._2)))

    val plan = new ScalaPlan(Seq(expression), queryName)
    plan.setDefaultParallelism(dop)

    plan
  }
}
