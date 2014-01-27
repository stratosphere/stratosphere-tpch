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

import org.joda.time.DateTime

import eu.stratosphere.scala._
import eu.stratosphere.scala.operators._
import eu.stratosphere.tpch.schema._

/**
 * Original query:
 *
 * {{{
 * select
 * 	o_year,
 *  sum(case
 *      when nation = ':NATION' then volume
 *      else 0
 *    	end) / sum(volume) as mkt_share
 * from
 * 	(select
 *    extract(year from o_orderdate) as o_year,
 *    l_extendedprice * (1 - l_discount) as volume,
 *    n2.n_name as nation
 *   from
 *    part,
 *    supplier,
 *    lineitem,
 *    orders,
 *    customer,
 *    nation n1,
 *    nation n2,
 *    region
 *   where
 *    p_partkey = l_partkey
 *    and s_suppkey = l_suppkey
 *    and l_orderkey = o_orderkey
 *    and o_custkey = c_custkey
 *    and c_nationkey = n1.n_nationkey
 *    and n1.n_regionkey = r_regionkey
 *    and r_name = ':REGION'
 *    and s_nationkey = n2.n_nationkey
 *    and o_orderdate between date '1995-01-01' and date '1996-12-31'
 *    and p_type = ':TYPE'
 *  ) as all_nations
 * group by
 * 	o_year
 * order by
 * 	o_year;
 * }}}
 *
 * @param dop Degree of parallelism
 * @param inPath Base input path
 * @param outPath Output path
 * @param nName Query parameter `NATION`
 * @param rName Query parameter `REGION`
 * @param pType Query parameter `TYPE`
 */
class TPCHQuery08(dop: Int, inPath: String, outPath: String, nName: String, rName: String, pType: String) extends TPCHQuery(8, dop, inPath, outPath) {

  def plan(): ScalaPlan = {
    
    val minDate = new DateTime(1995,1,1,0,0)
    val maxDate = new DateTime(1996,12,31,0,0)
    
    // read, filter and project base relations
    val orders = ( Order(inPath) 
    		       filter { o => TPCHQuery.string2date(o.orderDate).compareTo(minDate) >= 0 &&
                                 TPCHQuery.string2date(o.orderDate).compareTo(maxDate) <= 0 }
    			   map { o => (o.orderKey, o.custKey, TPCHQuery.string2date(o.orderDate).getYear()) }
                 )
                 
    val parts = ( Part(inPath)
                  filter { p => p.ptype.equals(pType) }
                  map { p => p.partKey }
                )
                 
    val lineitems = ( Lineitem(inPath) 
                      map { l => (l.orderKey, l.suppKey, l.partKey, (l.extendedPrice * (1.0 - l.discount))) }
                   )
                   
    val customers = ( Customer(inPath)
                      map { c => (c.custKey, c.nationKey) }
                   )
                   
    val suppliers = ( Supplier(inPath)
                      map { s => (s.suppKey, s.nationKey) }
                   )
                   
    val nationC = ( Nation(inPath)
                    map { n => (n.nationKey, n.regionKey, n.name) }
                 )
    
    val nationS = ( Nation(inPath)
                    map { n => (n.nationKey, n.name) }
                 )
                 
    val region = ( Region(inPath)
                   filter { r => r.name.equals(rName) }
                   map { r => r.regionKey }
                 )

    // filter orders by customer region
    val custOrders = ( region join nationC where { r => r } isEqualTo { n => n._2 } 
                       map { (r,n) => (n._1, n._3) }
                              join customers where { n => n._1 } isEqualTo { c => c._2 }
                       map { (n,c) => c._1 }
                              join orders where { c => c } isEqualTo { o => o._2 }
                       map { (c,o) => (o._1, o._3) }
                     )

    // join supplier and nation
    val suppN = ( nationS join suppliers where { n => n._1 } isEqualTo { s => s._2 }
                  map { (n,s) => (s._1, n._2) }
                )
    
    // join lineitems with orders and supplier, group by year, aggregate, and compute final result
    val result = ( lineitems join parts where { l => l._3 } isEqualTo { p => p } 
                   map { (l,p) => (l._1, l._2, l._4) }
                             join custOrders where { l => l._1 } isEqualTo { o => o._1 }
                   map { (l,o) => (o._2, l._2, l._3) }
                             join suppN where { l => l._2 } isEqualTo { s => s._1 }
                   map { (l,s) => if (s._2.equals(nName)) (l._1, l._3, l._3) else (l._1, 0.0, l._3) }
                   groupBy { l => l._1 }
                   reduce { (l1,l2) => (l1._1, (l1._2 + l2._2), (l1._3 + l2._3) ) }
                   map { l => (l._1, (l._2 / l._3) ) }
                 )
                  
    // write result
    val expression = result.write(s"$outPath/query08.result", DelimitedOutputFormat(x => "%d|%f".format(x._1, x._2)))

    val plan = new ScalaPlan(Seq(expression), queryName)
    plan.setDefaultParallelism(dop)

    plan
  }
}
