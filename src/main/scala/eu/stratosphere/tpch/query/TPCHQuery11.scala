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
 *  ps_partkey,
 *  sum(ps_supplycost * ps_availqty) as value
 * from
 *  partsupp,
 *  supplier,
 *  nation
 * where
 *  ps_suppkey = s_suppkey
 *  and s_nationkey = n_nationkey
 *  and n_name = ':NATION'
 * group by
 *  ps_partkey 
 * having sum(ps_supplycost * ps_availqty) > ( select
 *                                              sum(ps_supplycost * ps_availqty) * :FRACTION
 *                                             from
 *                                              partsupp,
 *                                              supplier,
 *                                              nation
 *                                             where 
 *                                              ps_suppkey = s_suppkey
 *                                              and s_nationkey = n_nationkey
 *                                              and n_name = ':NATION'
 *                                           )
 * order by
 *  value desc;
 * }}}
 *
 * @param dop Degree of parallelism
 * @param inPath Base input path
 * @param outPath Output path
 * @param nation Query parameter `NATION`
 * @param fraction Query parameter `FRACTION`
 */
class TPCHQuery11(dop: Int, inPath: String, outPath: String, nation: String, fraction: Double) extends TPCHQuery(11, dop, inPath, outPath) {

  // TODO: final order by not implemented yet.
  // TODO: cross should be replaced by a broadcast variable once available
  
  def plan(): ScalaPlan = {

    // scan partSupp and project
    val partSupp = ( PartSupp(inPath)
    				 map { ps => ( ps.suppKey, ps.partKey, (ps.availQty * ps.supplyCost) )}
                   )
    
    // scan supplier and project
    val supplier = ( Supplier(inPath)
                     map { s => ( s.suppKey, s.nationKey ) }
                   )
    
    // scan nation, filter, and project
    val nations = ( Nation(inPath)
                   filter { n => n.name.equals(nation) }
    			   map { n => n.nationKey }
                  )
                  
    // join supplier and nation
    val supNat = ( supplier join nations where { s => s._2 } isEqualTo { n => n }
                   map { (s, n) => s._1 }
                 )
    
    // join supplier, nation, and partSupp
    val supNatPart = ( supNat join partSupp where { s => s } isEqualTo { ps => ps._1 }
                       map { (s, ps) => (ps._2, ps._3) }
                     )

    // compute full nation value sum
    val nationVal = ( supNatPart reduce { (snp1, snp2) => (0, (snp1._2 + snp2._2)) } )
    
    // group by supplier, compute value sum
    val suppVal = ( supNatPart groupBy {snp => snp._1 }
                               reduce { (snp1, snp2) => (snp1._1, (snp1._2 + snp2._2)) }
                  )
                  
    // filter supplier by fraction of nation sum
    val result = ( nationVal cross suppVal
                   filter { (n, s) => (s._2 > (n._2 * fraction)) }
    			   map { x => (x._2._1, x._2._2) }
                 )
   
    // write result
    val out = result.write(s"$outPath/query11.result", DelimitedOutputFormat(x => "%d|%f".format(x._1, x._2)))

    val plan = new ScalaPlan(Seq(out), queryName)
    plan.setDefaultParallelism(dop)

    plan
  }
}
