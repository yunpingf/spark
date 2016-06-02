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
package org.apache.spark.executor

import java.io.{ObjectOutput, Externalizable, ObjectInput}
import java.util.concurrent.ConcurrentHashMap

import org.apache.spark.SparkContext
import org.apache.spark.util.Utils

class ExecutorStatsCollectorId private(
  private var executorId_ : String,
  private var host_ : String,
  private var port_ : Int) extends Externalizable{

  private def this() = this(null, null, 0)  // For deserialization only

  def executorId: String = executorId_

  if (host_ != null) {
    println(host_ + " " + port_)
    Utils.checkHost(host_, "Expected hostname")
    assert (port_ > 0)
  }

  def hostPort: String = {
    // DEBUG code
    Utils.checkHost(host)
    assert (port > 0)
    host + ":" + port
  }

  def host: String = host_

  def port: Int = port_

  def isDriver: Boolean = {
    executorId == SparkContext.DRIVER_IDENTIFIER ||
      executorId == SparkContext.LEGACY_DRIVER_IDENTIFIER
  }

  override def writeExternal(out: ObjectOutput): Unit = Utils.tryOrIOException {
    out.writeUTF(executorId_)
    out.writeUTF(host_)
    out.writeInt(port_)
  }

  override def readExternal(in: ObjectInput): Unit = Utils.tryOrIOException {
    executorId_ = in.readUTF()
    host_ = in.readUTF()
    port_ = in.readInt()
  }


  override def toString: String = s"ExecutorStatsCollectorId($executorId, $host, $port)"

  override def hashCode: Int = (executorId.hashCode * 41 + host.hashCode) * 41 + port

  override def equals(that: Any): Boolean = that match {
    case id: ExecutorStatsCollectorId =>
      executorId == id.executorId && port == id.port && host == id.host
    case _ =>
      false
  }
}

private[spark] object ExecutorStatsCollectorId {
  def apply(execId: String, host: String, port: Int): ExecutorStatsCollectorId =
    getCachedExecutorStatsCollectorId(new ExecutorStatsCollectorId(execId, host, port))

  def apply(in: ObjectInput): ExecutorStatsCollectorId = {
    val obj = new ExecutorStatsCollectorId()
    obj.readExternal(in)
    getCachedExecutorStatsCollectorId(obj)
  }

  val executorStatsCollectorIdCache =
    new ConcurrentHashMap[ExecutorStatsCollectorId, ExecutorStatsCollectorId]()

  def getCachedExecutorStatsCollectorId(id: ExecutorStatsCollectorId): ExecutorStatsCollectorId = {
    executorStatsCollectorIdCache.putIfAbsent(id, id)
    executorStatsCollectorIdCache.get(id)
  }
}
