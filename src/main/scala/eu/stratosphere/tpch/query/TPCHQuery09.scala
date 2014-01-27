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

import eu.stratosphere.pact.client.LocalExecutor

/**
 * Original query:
 *
 * {{{
 * select
 * nation,
 * o_year,
 * sum(amount) as sum_profit
 * from
 * (select
 *   n_name as nation,
 *   extract(year from o_orderdate) as o_year,
 *   l_extendedprice * (1 - l_discount) - ps_supplycost * l_quantity as amount
 *  from
 *   part,
 *   supplier,
 *   lineitem,
 *   partsupp,
 *   orders,
 *   nation
 *  where
 *   s_suppkey = l_suppkey
 *   and ps_suppkey = l_suppkey
 *   and ps_partkey = l_partkey 
 *   and p_partkey = l_partkey 
 *   and o_orderkey = l_orderkey
 *   and s_nationkey = n_nationkey
 *   and p_name like '%:COLOR%'
 * ) as profit
 * group by
 *  nation,
 *  o_year
 * order by
 * 	nation,
 * 	o_year desc;
 * }}}
 *
 * @param dop Degree of parallelism
 * @param inPath Base input path
 * @param outPath Output path
 * @param color Query parameter `COLOR`
 */
class TPCHQuery09(dop: Int, inPath: String, outPath: String, color: String) extends TPCHQuery(9, dop, inPath, outPath) {

   // TODO: order by year and nation missing
  
  def plan(): ScalaPlan = {
    
    // scan Part, filter, and project
    val part = ( Part(inPath) 
                 filter (p => p.name.indexOf(color) != -1)
                 map { p => p.partKey }
               )
               
    // scan supplier and project
    val supplier = ( Supplier(inPath)
                     map { s => (s.suppKey, s.nationKey) }
                   )

    // scan lineitem and project
    val lineitem = ( Lineitem(inPath)
                     map { l => (l.orderKey, l.suppKey, l.partKey, (l.extendedPrice * (1.0 - l.discount)), l.quantity) }
                   )

    // scan partsupp and project
    val partsupp = ( PartSupp(inPath)
                     map { ps => (ps.suppKey, ps.partKey, ps.supplyCost) }
                   )

    // scan orders and project
    val order = ( Order(inPath)
                  map { o => (o.orderKey, TPCHQuery.string2date(o.orderDate).getYear()) }
                )

    // scan nation and project
    val nation = ( Nation(inPath)
                   map { n => (n.nationKey, n.name) }
                 )

    // join lineitem and part                 
    val lp = ( lineitem join part where (_._3) isEqualTo ( p => p ) 
               map { (l, p) => l }
             )
    
   // join lineitem-part and partsupp
    val lpps = ( lp join partsupp where (lp => (lp._2, lp._3)) isEqualTo (ps => (ps._1 , ps._2) )
                 map { (lp, ps) => (lp._1, lp._2, (lp._4 - (ps._3 * lp._5)) )  }
	           )

    // join lineitem-part-partsupp and orders
	val lppso = ( lpps join order where ( _._1 ) isEqualTo ( _._1 ) 
	              map { (x, o) => (x._2, o._2, x._3) }
	            )

    // join lineitem-part-partsupp-orders and supplier, group by nation and year, and compute sum
    val lppsos = ( lppso join supplier where (_._1) isEqualTo (_._1) 
                   map { (x, s) => (s._2, x._2, x._3) }
    			   groupBy ( x => (x._1, x._2) )
    			   reduce { (x1, x2) => (x1._1, x1._2, (x1._3 + x2._3)) }
                 )

    // join with nation
    val lppsosn = ( lppsos join nation where ( _._1 ) isEqualTo ( _._1)
                    map { (x, n) => (n._2, x._2, x._3 ) }
                  )
   
    // write result
    val expression = lppsosn.write(s"$outPath/query09.result", DelimitedOutputFormat(x => "%s|%d|%f".format(x._1, x._2, x._3)))

    val plan = new ScalaPlan(Seq(expression), queryName)
    plan.setDefaultParallelism(dop)

    plan
  }
}

object RunQuery {
  def main(args: Array[String]) {
    val q = new TPCHQuery09(4, "file:///home/fhueske/tpch-s1/text", "file:///home/fhueske/result", "green")
    LocalExecutor.execute(q.plan)
  }
}
