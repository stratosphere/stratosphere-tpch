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
package eu.stratosphere.tpch

import eu.stratosphere.pact.client.LocalExecutor
import eu.stratosphere.pact.client.RemoteExecutor

import eu.stratosphere.tpch.query.TPCHConfig
import eu.stratosphere.tpch.query.TPCHQuery
import eu.stratosphere.tpch.query.TPCHQuery01
import eu.stratosphere.scala.ScalaPlan

abstract class RunJobBase {

  /**
   * Main method.
   */
  def main(args: Array[String]) {

    TPCHQuery.parser.parse(args, TPCHConfig()) map { config =>
      try {
        executeQuery(createQuery(config).plan())
      } catch {
        case e: Throwable => {
          System.err.println("Error during execution: " + e.getMessage())
          e.printStackTrace(System.err)
          System.exit(-1)
        }
      }
    } getOrElse {
      System.exit(-1)
    }
  }

  /**
   * Factory method for TPC-H Queries.
   */
  protected def createQuery(config: TPCHConfig) = config.queryNo match {
    case 1 => new TPCHQuery01(config.dop, config.inPath, config.outPath, config.delta)
  }

  /**
   * Executes the query in a specific environment (local or remote).
   */
  def executeQuery(plan: ScalaPlan)
}

// You can run TPCH Query X this on a cluster using:
// mvn exec:exec -Dexec.executable="java" -Dexec.args="-cp %classpath eu.stratosphere.tpch.RunJobLocal QXX 2 file:///tpch/path file:///query/result/path <Query-X-args>"
object RunJobLocal extends RunJobBase {

  def executeQuery(plan: ScalaPlan) {
    LocalExecutor.execute(plan)
  }
}

// You can run TPCH Query X this on a cluster using:
// mvn exec:exec -Dexec.executable="java" -Dexec.args="-cp %classpath eu.stratosphere.tpch.RunJobRemote QXX 2 file:///some/path file:///some/other/path <Query-X-args>"
object RunJobRemote extends RunJobBase {

  def executeQuery(plan: ScalaPlan) {
    new RemoteExecutor("localhost", 6123, "target/stratosphere-tpch-bin.jar").executePlan(plan)
  }
}
