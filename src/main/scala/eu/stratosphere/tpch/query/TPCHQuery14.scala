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
 *  100.00 * sum(
 *   case
 *    when p_type like 'PROMO%'
 *    then l_extendedprice * (1 - l_discount)
 *    else 0
 * 	 end
 *  ) /  
 * 	sum(l_extendedprice * (1 - l_discount)) as promo_revenue
 * from
 *  lineitem,
 *  part
 * where
 *  l_partkey = p_partkey 
 *  and l_shipdate >= date ':DATE'
 *  and l_shipdate < date ':DATE' + interval '1' month;
 * }}}
 *
 * @param dop Degree of parallelism
 * @param inPath Base input path
 * @param outPath Output path
 * @param date Query parameter `DATE`
 */
class TPCHQuery14(dop: Int, inPath: String, outPath: String, date: DateTime) extends TPCHQuery(14, dop, inPath, outPath) {

  def plan(): ScalaPlan = {

    // compute date bounds
    val dateLowBound  = date
    val dateHighBound = date.plusMonths(1)
    
    // scan part and project
    val part = ( Part(inPath)
                 map { p => ( p.partKey, p.ptype ) }
               )
    
    // scan lineitem, filter, and project
    val lineitem = ( Lineitem(inPath) 
                     filter { l => ( TPCHQuery.string2date(l.shipDate).compareTo(dateLowBound) >= 0 
                                     && TPCHQuery.string2date(l.shipDate).compareTo(dateHighBound) < 0
                                   )
                            }
                     map { l => ( l.partKey, (l.extendedPrice * ( 1.0 - l.discount ) ) ) }
                   )

    // join part and lineitem and project
    val pl = ( part join lineitem where { _._1 } isEqualTo { _._1 }  
               map { (p, l) => ( if ( p._2.startsWith("PROMO") ) 
                                   l._2 
                                 else 
                                   0.0,
                                 l._2
                               ) 
                   }
             )

    // compute sums
    val agg = ( pl reduce { (pl1, pl2) => ( (pl1._1 + pl2._1 ), (pl1._2 + pl2._2 ) ) } 
                map ( pl => 100 * (pl._1 / pl._2 ) )
              )

    // write result
    val out = agg.write(s"$outPath/query14.result", DelimitedOutputFormat(x => "%f".format(x)))

    val plan = new ScalaPlan(Seq(out), queryName)
    plan.setDefaultParallelism(dop)

    plan
  }
}
