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

/**
 * Original query:
 *
 * {{{
 * select
 * 	s_acctbal,
 * 	s_name,
 * 	n_name,
 * 	p_partkey,
 * 	p_mfgr,
 * 	s_address,
 * 	s_phone,
 * 	s_comment
 * from
 * 	part,
 * 	supplier,
 * 	partsupp,
 * 	nation,
 * 	region
 * where
 * 	p_partkey = ps_partkey
 * 	and s_suppkey = ps_suppkey
 * 	and p_size = :SIZE
 * 	and p_type like '%:TYPE'
 * 	and s_nationkey = n_nationkey
 * 	and n_regionkey = r_regionkey
 * 	and r_name = ':REGION'
 * 	and ps_supplycost = (
 * 		select
 * 			min(ps_supplycost)
 * 		from
 * 			partsupp,
 * 			supplier,
 * 			nation,
 * 			region
 * 		where
 * 			p_partkey = ps_partkey
 * 			and s_suppkey = ps_suppkey
 * 			and s_nationkey = n_nationkey
 * 			and n_regionkey = r_regionkey
 * 			and r_name = ':REGION'
 * 	)
 * order by
 * 	s_acctbal desc,
 * 	n_name,
 * 	s_name,
 * 	p_partkey;
 * }}}
 *
 * @param dop Degree of parallism
 * @param inPath Base input path
 * @param outPath Output path
 * @param size Query parameter `SIZE`
 * @param ptype Query parameter `TYPE`
 * @param region Query parameter `REGION`
 */
class TPCHQuery02(dop: Int, inPath: String, outPath: String, size: Int, ptype: String, region: String) extends TPCHQuery(2, dop, inPath, outPath) {

  def plan(): ScalaPlan = {

    val region = Region(inPath) filter (_.name == this.region)
    val nation = Nation(inPath)
    val supplier = Supplier(inPath)
    val partsupp = PartSupp(inPath)
    val part = Part(inPath) filter (p => p.size == this.size && p.ptype.indexOf(ptype) == p.ptype.length - this.ptype.length)

    val e1 = region join nation where (_.regionKey) isEqualTo (_.regionKey) map {
      (r, n) => (n.name, n.nationKey)
    }
    val e2 = e1 join supplier where (_._2) isEqualTo (_.nationKey) map {
      (x, y) => (y.accBal, y.name, x._1, y.address, y.phone, y.comment, y.suppKey)
    }
    val e3 = e2 join partsupp where (_._7) isEqualTo (_.suppKey) map {
      (x, y) => (x._1, x._2, x._3, x._4, x._5, x._6, y.supplyCost, y.partKey)
    } groupBy (_._8) reduce {
      (x, y) => if (x._7 < y._7) x else y
    }
    val e4 = e3 join part where (_._8) isEqualTo (_.partKey) map {
      (x, y) => (x._1, x._2, x._3, y.partKey, y.mfgr, x._4, x._5, x._6)
    }
    // TODO: sort e4 on (_1 desc, _2 asc, _3 asc, _4 asc)

    val expression = e4.write(s"$outPath/query02.result", DelimitedOutputFormat(x => "%f|%s|%s|%d|%s|%s|%s|%s".format(x._1, x._2, x._3, x._4, x._5, x._6, x._7, x._8)))

    val plan = new ScalaPlan(Seq(expression), queryName)
    plan.setDefaultParallelism(dop)

    plan
  }
}