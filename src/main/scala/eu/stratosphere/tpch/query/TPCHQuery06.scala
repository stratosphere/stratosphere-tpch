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
 * 	sum(l_extendedprice * l_discount) as revenue
 * from
 * 	lineitem
 * where
 * 	l_shipdate >= date ':DATE'
 *  and l_shipdate < date ':DATE' + interval '1' year
 *  and l_discount between :DISCOUNT - 0.01 and :DISCOUNT + 0.01
 *  and l_quantity < :QUANTITY;
  * }}}
 *
 * @param dop Degree of parallelism
 * @param inPath Base input path
 * @param outPath Output path
 * @param date Query parameter `DATE`
 * @param discount Query parameter `DISCOUNT`
 * @param quantity Query parameter `QUANTITY`
 * 
 */
class TPCHQuery06(dop: Int, inPath: String, outPath: String, date: DateTime, discount: Double, quantity: Int ) extends TPCHQuery(6, dop, inPath, outPath) {

  def plan(): ScalaPlan = {
    
    val minDate = date
    val maxDate = date.plusYears(1)

    val diff = (BigDecimal(1)./(BigDecimal(100)))
    val minDiscount = (BigDecimal.apply(discount).-(diff)).toDouble
    val maxDiscount = (BigDecimal.apply(discount).+(diff)).toDouble
    
    // read, filter, project, and aggregate lineitems
    val result = ( Lineitem(inPath) 
    		       filter { l => l.discount >= minDiscount &&
                                 l.discount <= maxDiscount &&
                                 l.quantity < quantity && 
    			                 TPCHQuery.string2date(l.shipDate).compareTo(minDate) >= 0 &&
                                 TPCHQuery.string2date(l.shipDate).compareTo(maxDate) < 0 
                          }
                   map { l => (l.extendedPrice * l.discount) }
                   reduce { (l1, l2) => (l1 + l2) }
                 )
       
    // write result
    val expression = result.write(s"$outPath/query06.result", DelimitedOutputFormat(x => "%f".format(x)))

    val plan = new ScalaPlan(Seq(expression), queryName)
    plan.setDefaultParallelism(dop)

    plan
  }
}

