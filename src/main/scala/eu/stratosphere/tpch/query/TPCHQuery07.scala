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
 *  supp_nation,
 *  cust_nation,
 *  l_year,
 *  sum(volume) as revenue
 * from	
 *  (select
 *    n1.n_name as supp_nation,
 *    n2.n_name as cust_nation,
 *    extract(year from l_shipdate) as l_year,
 *    l_extendedprice * (1 - l_discount) as volume
 *   from
 *    supplier,
 *    lineitem,
 *    orders,
 *    customer,
 *    nation n1,
 *    nation n2
 *   where
 *    s_suppkey = l_suppkey
 *    and o_orderkey = l_orderkey
 *    and c_custkey = o_custkey
 *    and s_nationkey = n1.n_nationkey
 *    and c_nationkey = n2.n_nationkey
 *    and (
 *    	(n1.n_name = ':1' and n2.n_name = ':2')
 *      or (n1.n_name = ':2' and n2.n_name = ':1')
 *        )
 *    and l_shipdate between date '1995-01-01' and date '1996-12-31'
 *  ) as shipping
 * group by
 *  supp_nation,
 *  cust_nation,
 *  l_year
 * order by
 * 	supp_nation,
 *  cust_nation,
 *  l_year;
 * }}}
 *
 * @param dop Degree of parallelism
 * @param inPath Base input path
 * @param outPath Output path
 * @param nName1 Query parameter `NATION1`
 * @param nName2 Query parameter `NATION2`
 */
class TPCHQuery07(dop: Int, inPath: String, outPath: String, nName1: String, nName2: String) extends TPCHQuery(7, dop, inPath, outPath) {

  def plan(): ScalaPlan = {
    
    val minDate = new DateTime(1995,1,1,0,0)
    val maxDate = new DateTime(1996,12,31,0,0)
    
    // read, filter and project base relations
    val customers = ( Customer(inPath)
                      map { c => (c.custKey, c.nationKey) }
                   )

    val orders = ( Order(inPath) 
    			   map { o => (o.orderKey, o.custKey) }
                 )
                 
    val lineitems = ( Lineitem(inPath) 
    	              filter { l => TPCHQuery.string2date(l.shipDate).compareTo(minDate) >= 0 &&
                                    TPCHQuery.string2date(l.shipDate).compareTo(maxDate) <= 0 }
                      map { l => (l.orderKey, l.suppKey, 
                                  TPCHQuery.string2date(l.shipDate).getYear(), 
                                  (l.extendedPrice * (1.0 - l.discount))) 
                          }
                   )
                   
    val suppliers = ( Supplier(inPath)
                      map { s => (s.suppKey, s.nationKey) }
                   )
                   
    val nation1 = ( Nation(inPath)
    				filter { n => n.name.equals(nName1) || n.name.equals(nName2) }
                    map { n => (n.nationKey, n.name) }
                 )
    
    val nation2 = ( Nation(inPath)
    				filter { n => n.name.equals(nName1) || n.name.equals(nName2) }
                    map { n => (n.nationKey, n.name) }
                 )
                 
    val nations = ( nation1 cross nation2
                    map { (n1, n2) => (n1._1, n2._1, n1._2, n2._2) }
                    filter { n => n._1 != n._2 }
                  )
                  
    // join base relations and compute sum
    val result = ( nations join suppliers where { n => n._1 } isEqualTo { s => s._2 }
                           map { (n,s) => (n._3, n._2, n._4, s._1) }
                           join lineitems where { s => s._4 } isEqualTo { l => l._2 }
                           map { (s,l) => (s._1, s._2, s._3, l._1, l._3, l._4) }
                           join orders where { l => l._4 } isEqualTo { o => o._1 }
                           map { (l,o) => (l._1, l._2, l._3, o._2, l._5, l._6) }
                           join customers where { l=> (l._4, l._2) } isEqualTo { c => (c._1, c._2) }
                           map { (l,c) => (l._1, l._3, l._5, l._6) }
                           groupBy { l => (l._1, l._2, l._3) }
                           reduce { (l1,l2) => (l1._1, l1._2, l1._3, (l1._4 + l2._4) )}
                 )
              
    // write result
    val expression = result.write(s"$outPath/query07.result", DelimitedOutputFormat(x => "%s|%s|%d|%f".format(x._1, x._2, x._3, x._4)))

    val plan = new ScalaPlan(Seq(expression), queryName)
    plan.setDefaultParallelism(dop)

    plan
  }
}
