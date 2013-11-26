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
package eu.stratosphere.tpch.config

import scopt.OptionParser

/**
 * Configuration case class populated by the option parser.
 */
case class TPCHConfig(
  // generic parameters
  queryNo: Int = 1,
  dop: Int = 1,
  inPath: String = "",
  outPath: String = "",
  // query parameters defined in the TPC-H specification
  brands: List[String] = List(),
  color: String = "",
  container: String = "",
  date: String = "",
  delta: Int = 0,
  discount: Double = 0.00,
  fraction: Double = 0.00,
  codes: List[String] = List(),
  nations: List[String] = List(),
  quantities: List[Int] = List(),
  region: String = "",
  segment: String = "",
  shipmodes: List[String] = List(),
  sizes: List[Int] = List(),
  ptype: String = "",
  words: List[String] = List())