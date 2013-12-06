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
 * c_name,
 * c_custkey, 
 * o_orderkey,
 * o_orderdate,
 * o_totalprice,
 * sum(l_quantity)
 * from 
 * customer,
 * orders,
 * lineitem
 * where 
 * o_orderkey in (
 * select
 * l_orderkey
 * from
 * lineitem
 * group by 
 * l_orderkey having 
 * sum(l_quantity) > [QUANTITY]
 * )
 * and c_custkey = o_custkey
 * and o_orderkey = l_orderkey
 * group by 
 * c_name, 
 * c_custkey, 
 * o_orderkey, 
 * o_orderdate, 
 * o_totalprice
 * order by 
 * o_totalprice desc,
 * o_orderdate;
 * }}}
 *
 * @param dop Degree of parallism
 * @param inPath Base input path
 * @param outPath Output path
 * @param quantities Query parameter `QUANTITY` -> default: 300
 */
class TPCHQuery18(dop: Int, inPath: String, outPath: String, quantities: List[Int]) extends TPCHQuery(18, dop, inPath, outPath) {

  def plan(): ScalaPlan = {

    val customer = Customer(inPath)
    val order = Order(inPath)
    val lineitem = Lineitem(inPath)
    
    val inner_select = lineitem map {
      l => (l.orderKey, l.quantity)
    } groupBy (_._1) reduce {
      (x, y) => (x._1, x._1 + y._1)
    } filter (_._2 > quantities.head)


    var e3 = order join inner_select where { _.orderKey } isEqualTo { _._1 } map {
      (o, y) => (o.orderKey, o.orderDate, o.totalPrice, o.custKey)
    }
    
    val e1 = customer join e3 where { _.custKey } isEqualTo { _._4 } map {
      (c, o) => (o._1, o._2, o._3, c.name , c.custKey)
    }
    
    val e2 = e1 join lineitem where { _._1 } isEqualTo { _.orderKey } map {
      (x, l) => (x._1,  x._2, x._3, x._4, x._5, l.quantity)
    } groupBy( x => (x._4, x._5, x._1,  x._2, x._3) ) reduce {
      (x, y) => (x._1, x._2, x._3,  x._4, x._5, x._6 + y._6)
    }
     
    // TODO: sort, limit to 100

    val expression = e2.write(s"$outPath/query18.result", DelimitedOutputFormat(x => "%s|%d|%d|%s|%f|%d".format(x._4, x._5, x._1, x._2, x._3, x._6)))
    
    val plan = new ScalaPlan(Seq(expression), queryName)
    plan.setDefaultParallelism(dop)

    plan
  }
}