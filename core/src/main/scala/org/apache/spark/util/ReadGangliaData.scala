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

import scala.collection.mutable.HashMap
import scala.io.Source
import scala.xml._

object ReadGangliaData {
  type MetricInfo = (String, (String, String, String))

  private def readFromSocket(ip: String): String = {
    val connection = new Socket(ip, 8649)
    val inStream = connection.getInputStream
    Source.fromInputStream(inStream).mkString
  }

  def readXmlMetrics(ip: String): HashMap[String, List[MetricInfo]] = {
    val xml_data = XML.loadString(readFromSocket(ip))
    val ganglia_xml = xml_data \\ "GANGLIA_XML"
    val host = ganglia_xml \ "CLUSTER" \ "HOST"
    val host_name = (host \ "@NAME").text
    val host_ip = (host \ "@IP").text
    val metrics = host \ "METRIC"

    val attrs = HashMap[String, List[MetricInfo]]()
    def filterAttribute(node: Node, att: String, value: String) =
      (node \ ("@" + att)).text == value

    for (m <- metrics) {
      val extra_elems = m \ "EXTRA_DATA" \ "EXTRA_ELEMENT"
      val group = (extra_elems.filter(n =>
        filterAttribute(n, "NAME", "GROUP")).head \ "@VAL").text

      val metric_name = (m \ "@NAME").text
      val metric_value = (m \ "@VAL").text
      val metric_type = (m \ "@TYPE").text
      val metric_units = (m \ "@UNITS").text

      val minfo = (metric_name ->
        (metric_value, metric_type, metric_units))
      attrs.put(group, minfo :: attrs.getOrElse(group, Nil))
    }

    attrs
  }

  def main(args: Array[String]): Unit = {
    // scalastyle:off println
    println(readXmlMetrics("localhost"))
    // scalastyle:on println
  }
}

