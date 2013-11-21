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
package scopt

import collection.mutable.{ ListBuffer, ListMap }
import eu.stratosphere.tpch.config.TPCHConfig

/**
 * Scopt OptionParser specialization for this application.
 */
class TPCHOptionParser extends OptionParser[TPCHConfig]("stratosphere-tpch") {

  val NL = System.getProperty("line.separator")
  
  override def showUsageOnError = false

  {
    // header
    head("Stratosphere TPC-H Query Runner", "(version 1.0.0)")

    // help option
    help("help")

    cmd("Q01")
      .text("Run TPC-H Query #01" + NL)
      .action { (x, c) => c.copy(queryNo = 1) }
      .children(commonQueryArgs(this): _*)
      .children(
        arg[Int]("[delta]")
          .action { (x, c) => c.copy(`delta` = x) }
          .text("TPC-H query parameter" + NL))

    cmd("Q02")
      .text("Run TPC-H Query #02" + NL)
      .action { (x, c) => c.copy(queryNo = 2) }
      .children(commonQueryArgs(this): _*)
      .children(
        arg[Int]("[size]")
          .action { (x, c) => c.copy(`sizes` = List(x)) }
          .text("TPC-H query parameter"),
        arg[String]("[type]")
          .action { (x, c) => c.copy(`type` = x) }
          .text("TPC-H query parameter"),
        arg[String]("[region]")
          .action { (x, c) => c.copy(`region` = x) }
          .text("TPC-H query parameter" + NL))

    cmd("Q03")
      .text("Run TPC-H Query #03" + NL)
      .action { (x, c) => c.copy(queryNo = 3) }
      .children(commonQueryArgs(this): _*)
      .children(
        arg[String]("[segment]")
          .action { (x, c) => c.copy(`segment` = x) }
          .text("TPC-H query parameter"),
        arg[String]("[date]")
          .action { (x, c) => c.copy(`date` = x) }
          .text("TPC-H query parameter" + NL))

    cmd("Q04")
      .text("Run TPC-H Query #04" + NL)
      .action { (x, c) => c.copy(queryNo = 4) }
      .children(commonQueryArgs(this): _*)
      .children(
        arg[String]("[out]")
          .action { (x, c) => c.copy(outPath = x) }
          .text("Output path for the query result"),
        arg[String]("[date]")
          .action { (x, c) => c.copy(`date` = x) }
          .text("TPC-H query parameter" + NL))

    cmd("Q05")
      .text("Run TPC-H Query #05" + NL)
      .action { (x, c) => c.copy(queryNo = 5) }
      .children(commonQueryArgs(this): _*)
      .children(
        arg[String]("[region]")
          .action { (x, c) => c.copy(`region` = x) }
          .text("TPC-H query parameter"),
        arg[String]("[date]")
          .action { (x, c) => c.copy(`date` = x) }
          .text("TPC-H query parameter" + NL))

    cmd("Q06")
      .text("Run TPC-H Query #06" + NL)
      .action { (x, c) => c.copy(queryNo = 6) }
      .children(commonQueryArgs(this): _*)
      .children(
        arg[String]("[date]")
          .action { (x, c) => c.copy(`date` = x) }
          .text("TPC-H query parameter"),
        arg[Double]("[discount]")
          .action { (x, c) => c.copy(`discount` = x) }
          .text("TPC-H query parameter"),
        arg[Int]("[quantity]")
          .action { (x, c) => c.copy(`quantities` = List(x)) }
          .text("TPC-H query parameter" + NL))

    cmd("Q07")
      .text("Run TPC-H Query #07" + NL)
      .action { (x, c) => c.copy(queryNo = 7) }
      .children(commonQueryArgs(this): _*)
      .children(
        arg[String]("[nation1]")
          .action { (x, c) => c.copy(`nations` = List(x)) }
          .text("TPC-H query parameter"),
        arg[String]("[nation2]")
          .action { (x, c) => c.copy(`nations` = List(x)) }
          .text("TPC-H query parameter" + NL))

    cmd("Q08")
      .text("Run TPC-H Query #08" + NL)
      .action { (x, c) => c.copy(queryNo = 8) }
      .children(commonQueryArgs(this): _*)
      .children(
        arg[String]("[nation]")
          .action { (x, c) => c.copy(`nations` = List(x)) }
          .text("TPC-H query parameter"),
        arg[String]("[region]")
          .action { (x, c) => c.copy(`region` = x) }
          .text("TPC-H query parameter"),
        arg[String]("[type]")
          .action { (x, c) => c.copy(`type` = x) }
          .text("TPC-H query parameter" + NL))

    cmd("Q09")
      .text("Run TPC-H Query #09" + NL)
      .action { (x, c) => c.copy(queryNo = 9) }
      .children(commonQueryArgs(this): _*)
      .children(
        arg[String]("[color]")
          .action { (x, c) => c.copy(`color` = x) }
          .text("TPC-H query parameter" + NL))

    cmd("Q10")
      .text("Run TPC-H Query #10" + NL)
      .action { (x, c) => c.copy(queryNo = 10) }
      .children(commonQueryArgs(this): _*)
      .children(
        arg[String]("[date]")
          .action { (x, c) => c.copy(`date` = x) }
          .text("TPC-H query parameter" + NL))

    cmd("Q11")
      .text("Run TPC-H Query #11" + NL)
      .action { (x, c) => c.copy(queryNo = 11) }
      .children(commonQueryArgs(this): _*)
      .children(
        arg[String]("[nation]")
          .action { (x, c) => c.copy(`nations` = List(x)) }
          .text("TPC-H query parameter"),
        arg[Double]("[fraction]")
          .action { (x, c) => c.copy(`fraction` = x) }
          .text("TPC-H query parameter" + NL))

    cmd("Q12")
      .text("Run TPC-H Query #12" + NL)
      .action { (x, c) => c.copy(queryNo = 12) }
      .children(commonQueryArgs(this): _*)
      .children(
        arg[String]("[shipmode1]")
          .action { (x, c) => c.copy(`shipmodes` = List(x)) }
          .text("TPC-H query parameter"),
        arg[String]("[shipmode2]")
          .action { (x, c) => c.copy(`shipmodes` = List(x)) }
          .text("TPC-H query parameter"),
        arg[String]("[date]")
          .action { (x, c) => c.copy(`date` = x) }
          .text("TPC-H query parameter" + NL))

    cmd("Q13")
      .text("Run TPC-H Query #13" + NL)
      .action { (x, c) => c.copy(queryNo = 13) }
      .children(commonQueryArgs(this): _*)
      .children(
        arg[String]("[words]")
          .action { (x, c) => c.copy(`shipmodes` = c.words :+ x) }
          .minOccurs(2)
          .maxOccurs(2)
          .text("TPC-H query parameter" + NL))

    cmd("Q14")
      .text("Run TPC-H Query #14" + NL)
      .action { (x, c) => c.copy(queryNo = 14) }
      .children(commonQueryArgs(this): _*)
      .children(
        arg[String]("[date]")
          .action { (x, c) => c.copy(`date` = x) }
          .text("TPC-H query parameter" + NL))

    cmd("Q15")
      .text("Run TPC-H Query #15" + NL)
      .action { (x, c) => c.copy(queryNo = 15) }
      .children(commonQueryArgs(this): _*)
      .children(
        arg[String]("[date]")
          .action { (x, c) => c.copy(`date` = x) }
          .text("TPC-H query parameter" + NL))

    cmd("Q16")
      .text("Run TPC-H Query #16" + NL)
      .action { (x, c) => c.copy(queryNo = 16) }
      .children(commonQueryArgs(this): _*)
      .children(
        arg[String]("[brand]")
          .action { (x, c) => c.copy(`brands` = List(x)) }
          .text("TPC-H query parameter"),
        arg[String]("[type]")
          .action { (x, c) => c.copy(`type` = x) }
          .text("TPC-H query parameter"),
        arg[Int]("[sizes]")
          .action { (x, c) => c.copy(`sizes` = c.sizes :+ x) }
          .minOccurs(8)
          .maxOccurs(8)
          .text("TPC-H query parameters" + NL))

    cmd("Q17")
      .text("Run TPC-H Query #17" + NL)
      .action { (x, c) => c.copy(queryNo = 17) }
      .children(commonQueryArgs(this): _*)
      .children(
        arg[String]("[brand]")
          .action { (x, c) => c.copy(`brands` = List(x)) }
          .text("TPC-H query parameter"),
        arg[String]("[container]")
          .action { (x, c) => c.copy(`container` = x) }
          .text("TPC-H query parameter" + NL))

    cmd("Q18")
      .text("Run TPC-H Query #18" + NL)
      .action { (x, c) => c.copy(queryNo = 18) }
      .children(commonQueryArgs(this): _*)
      .children(
        arg[Int]("[quantity]")
          .action { (x, c) => c.copy(`quantities` = List(x)) }
          .text("TPC-H query parameter" + NL))

    cmd("Q19")
      .text("Run TPC-H Query #19" + NL)
      .action { (x, c) => c.copy(queryNo = 19) }
      .children(commonQueryArgs(this): _*)
      .children(
        arg[Int]("[quantities]")
          .action { (x, c) => c.copy(`quantities` = c.quantities :+ x) }
          .minOccurs(3)
          .maxOccurs(3)
          .text("TPC-H query parameter"),
        arg[String]("[brands]")
          .action { (x, c) => c.copy(`brands` = c.brands :+ x) }
          .minOccurs(3)
          .maxOccurs(3)
          .text("TPC-H query parameter" + NL))

    cmd("Q20")
      .text("Run TPC-H Query #20" + NL)
      .action { (x, c) => c.copy(queryNo = 20) }
      .children(commonQueryArgs(this): _*)
      .children(
        arg[String]("[color]")
          .action { (x, c) => c.copy(`color` = x) }
          .text("TPC-H query parameter"),
        arg[String]("[date]")
          .action { (x, c) => c.copy(`date` = x) }
          .text("TPC-H query parameter"),
        arg[String]("[nation]")
          .action { (x, c) => c.copy(`nations` = List(x)) }
          .text("TPC-H query parameter" + NL))

    cmd("Q21")
      .text("Run TPC-H Query #21" + NL)
      .action { (x, c) => c.copy(queryNo = 21) }
      .children(commonQueryArgs(this): _*)
      .children(
        arg[String]("[nation]")
          .action { (x, c) => c.copy(`nations` = List(x)) }
          .text("TPC-H query parameter" + NL))

    cmd("Q22")
      .text("Run TPC-H Query #22" + NL)
      .action { (x, c) => c.copy(queryNo = 22) }
      .children(commonQueryArgs(this): _*)
      .children(
        arg[String]("[codes]")
          .action { (x, c) => c.copy(`codes` = c.codes :+ x) }
          .text("TPC-H query parameter" + NL))
  }

  override private[scopt] def commandExample(cmd: Option[OptionDef[_, TPCHConfig]]): String = {
    val text = new ListBuffer[String]()
    text += cmd map { commandName } getOrElse programName
    val parentId = cmd map { _.id }
    val cs = commands filter { _.getParentId == parentId }
    if (cs.nonEmpty) text += "[Q01|Q02|...|Q22]"
    val os = options.toSeq filter { case x => x.kind == Opt && x.getParentId == parentId }
    val as = arguments filter { _.getParentId == parentId }
    if (os.nonEmpty) text += "[options]"
    if (cs exists { case x => arguments exists { _.getParentId == Some(x.id) } }) text += "<args>..."
    else if (as.nonEmpty) text ++= as map { _.argName }
    text.mkString(" ")
  }

  private def commonQueryArgs(parser: scopt.OptionParser[TPCHConfig]): Seq[scopt.OptionDef[_, TPCHConfig]] = Seq(
    parser.arg[Int]("[dop]")
      .action { (x, c) => c.copy(dop = x) }
      .text("Degree of parallelism"),
    parser.arg[String]("[in]")
      .action { (x, c) => c.copy(inPath = x) }
      .text("Base path for the TPC-H inputs"),
    parser.arg[String]("[out]")
      .action { (x, c) => c.copy(outPath = x) }
      .text("Output path for the query result"))
}

/*~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~*
 * Companion object
 *~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~*/

object TPCHOptionParser {

  def apply() = new TPCHOptionParser()
}