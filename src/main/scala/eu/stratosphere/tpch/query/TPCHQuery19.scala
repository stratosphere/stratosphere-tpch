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
 *  sum(l_extendedprice* (1 - l_discount)) as revenue
 * from
 *  lineitem,
 *  part
 * where 
 *  (
 *   p_partkey = l_partkey
 *   and p_brand = ':BRAND_1'
 *   and p_container in ('SM CASE', 'SM BOX', 'SM PACK', 'SM PKG')
 *   and l_quantity >= :QUANTITY_1 and l_quantity <= :QUANTITY_1 + 10
 *   and p_size between 1 and 5
 *   and l_shipmode in ('AIR', 'AIR REG')
 *   and l_shipinstruct = 'DELIVER IN PERSON'
 *  ) or (
 *   p_partkey = l_partkey
 *   and p_brand = ':BRAND_2'
 *   and p_container in ('MED BAG', 'MED BOX', 'MED PKG', 'MED PACK')
 *   and l_quantity >= :QUANTITY_2 and l_quantity <= :QUANTITY_2 + 10
 *   and p_size between 1 and 10
 *   and l_shipmode in ('AIR', 'AIR REG')
 *   and l_shipinstruct = 'DELIVER IN PERSON'
 *  ) or (
 *   p_partkey = l_partkey
 *   and p_brand = ':BRAND_3'
 *   and p_container in ('LG CASE', 'LG BOX', 'LG PACK', 'LG PKG')
 *   and l_quantity >= :QUANTITY_3 and l_quantity <= :QUANTITY_3 + 10
 *   and p_size between 1 and 15
 *   and l_shipmode in ('AIR', 'AIR REG')
 *   and l_shipinstruct = 'DELIVER IN PERSON'
 *  );
 * }}}
 *
 * @param dop Degree of parallelism
 * @param inPath Base input path
 * @param outPath Output path
 * @param brand1 parameter `BRAND_1`
 * @param quantity1 parameter `QUANTITY_1`
 * @param brand2 parameter `BRAND_2`
 * @param quantity2 parameter `QUANTITY_2`
 * @param brand3 parameter `BRAND_3`
 * @param quantity3 parameter `QUANTITY_3`
 */
class TPCHQuery19(dop: Int, inPath: String, outPath: String, 
				  brand1: String, quantity1: Int,
				  brand2: String, quantity2: Int,
				  brand3: String, quantity3: Int) extends TPCHQuery(19, dop, inPath, outPath) {

  def plan(): ScalaPlan = {

    // Scan Part and project
    val part = ( Part(inPath)
    			 map { p => ( p.partKey, p.brand, p.container, p.size ) }
               )

    // Scan Lineitem, filter, and project
    val lineitem = ( Lineitem(inPath) 
                     filter { l => l.shipInstruct.equals("DELIVER IN PERSON") && 
                                   ( l.shipMode.equals("AIR") || l.shipMode.equals("AIR REG") ) 
                            } 
                     map { l => (l.partKey, l.quantity, ( l.extendedPrice * ( 1.0d - l.discount ) ) ) }
                   )
    
    // Join Part and Lineitem, filter, and project
    val pl = ( part join lineitem where { _._1 } isEqualTo { _._1 }
               map { ( p, l ) => ( p._2, p._3, p._4, l._2, l._3 ) } 
               filter { pl => ( pl._4 >= quantity1 && pl._4 <= ( quantity1 + 10 )
                                && pl._3 >= 1 && pl._3 <= 5
                                &&  pl._1.equals(brand1)
                                && ( pl._2.equals("SM CASE") || pl._2.equals("SM BOX") || pl._2.equals("SM PACK") || pl._2.equals("SM PKG") )
                              ) || (     
                                pl._4 >= quantity2 && pl._4 <= ( quantity2 + 10 )
                                && pl._3 >= 1 && pl._3 <= 10 
                                && pl._1.equals(brand2) 
                                && ( pl._2.equals("MED BAG") || pl._2.equals("MED BOX") || pl._2.equals("MED PKG") || pl._2.equals("MED PACK") )
                              ) || ( 
                                pl._4 >= quantity3 && pl._4 <= ( quantity3 + 10 ) 
                                && pl._3 >= 1 && pl._3 <= 15 
                                && pl._1.equals(brand3) 
                                && ( pl._2.equals("LG CASE") || pl._2.equals("LG BOX") || pl._2.equals("LG PACK") || pl._2.equals("LG PKG") )
                              )
                      }
               map { pl => pl._5 }
             )
        
    // Sum revenue
    val agg = pl reduce { (l1, l2) =>  l1 + l2 }

    // Write result
    val out = agg.write(s"$outPath/query19.result", DelimitedOutputFormat(x => "%f".format(x)))

    val plan = new ScalaPlan(Seq(out), queryName)
    plan.setDefaultParallelism(dop)

    plan
  }
}
