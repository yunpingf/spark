/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.spark.util

import java.net.Socket

import org.apache.spark.executor.ExecutorStatsMetrics

import scala.io.Source
import scala.xml._


object ReadGangliaData {
  type MetricInfo = (String, (String, String, String))
  val CPU_SYSTEM = "cpu_system"
  val LOAD_ONE = "load_one"
  val LOAD_FIVE = "load_five"
  val LOAD_FIFTEEN = "load_fifteen"
  val CPU_A_IDLE = "cpu_aidle"
  val CPU_WIO = "cpu_wio"
  val CPU_USER = "cpu_user"
  val CPU_IDLE = "cpu_idle"
  val MEM_BUFFERS = "mem_buffers"
  val MEM_CACHED = "mem_cached"
  val MEM_FREE = "mem_free"
  val SWAP_FREE = "swap_free"
  val SWAP_TOTAL = "swap_total"

  private def readFromSocket(ip: String): String = {
    val connection = new Socket(ip, 8649)
    val inStream = connection.getInputStream
    Source.fromInputStream(inStream).mkString
  }

  def readXmlMetrics(ip: String): ExecutorStatsMetrics = {
    val xml_data = XML.loadString(readFromSocket(ip))
    val ganglia_xml = xml_data \\ "GANGLIA_XML"
    val host = ganglia_xml \ "CLUSTER" \ "HOST"
    val host_name = (host \ "@NAME").text
    val host_ip = (host \ "@IP").text
    val metrics = host \ "METRIC"

    def filterAttribute(node: Node, att: String, value: String) =
      (node \ ("@" + att)).text == value
    val esm = ExecutorStatsMetrics.empty

    for (m <- metrics) {
      val metric_name = (m \ "@NAME").text
      val metric_value = (m \ "@VAL").text
      metric_name match {
        case CPU_SYSTEM => esm.setCpuSystem(metric_value.toFloat)
        case LOAD_ONE => esm.setLoadOne(metric_value.toFloat)
        case LOAD_FIVE => esm.setLoadFive(metric_value.toFloat)
        case LOAD_FIFTEEN => esm.setLoadFifteen(metric_value.toFloat)
        case CPU_A_IDLE => esm.setCpuAIdle(metric_value.toFloat)
        case CPU_WIO => esm.setCpuWIO(metric_value.toFloat)
        case CPU_USER => esm.setCpuUser(metric_value.toFloat)
        case CPU_IDLE => esm.setCpuIdle(metric_value.toFloat)
        case MEM_BUFFERS => esm.setMemBuffers(metric_value.toFloat)
        case MEM_CACHED => esm.setMemCached(metric_value.toFloat)
        case MEM_FREE => esm.setMemFree(metric_value.toFloat)
        case SWAP_FREE => esm.setSwapFree(metric_value.toFloat)
        case SWAP_TOTAL => esm.setSwapTotal(metric_value.toFloat)
      }
    }
    esm
  }

  def main(args: Array[String]): Unit = {
    // scalastyle:off println
    println(readXmlMetrics("localhost"))
    // scalastyle:on println
  }
}

