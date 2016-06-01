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

import org.apache.spark.internal.Logging
import org.apache.spark.network.BlockTransferService
import org.apache.spark.shuffle.ShuffleManager
import org.apache.spark.util.IdGenerator
import org.apache.spark.{SecurityManager, MapOutputTracker, SparkConf}
import org.apache.spark.memory.MemoryManager
import org.apache.spark.rpc.RpcEnv
import org.apache.spark.serializer.SerializerManager
import org.apache.spark.storage.{BlockManagerSlaveEndpoint, BlockManagerMaster}
import org.apache.spark.SparkContext

class ExecutorStatsCollector(
  executorId: String,
  rpcEnv: RpcEnv,
  val master: ExecutorStatsCollectorMaster,
  serializerManager: SerializerManager,
  val conf: SparkConf,
  memoryManager: MemoryManager,
  mapOutputTracker: MapOutputTracker,
  shuffleManager: ShuffleManager,
  hostname: String,
  port: Int,
  securityManager: SecurityManager,
  numUsableCores: Int) extends Logging {
  private val slaveEndpoint = rpcEnv.setupEndpoint(
    "ExecutorStatsEndpoint" + ExecutorStatsCollector.ID_GENERATOR.next,
    new ExecutorStatsSlaveEndpoint(rpcEnv, this, mapOutputTracker))

  var executorStatsCollectorId: ExecutorStatsCollectorId = _

  def initialize(appId: String): Unit = {
    executorStatsCollectorId = ExecutorStatsCollectorId(
      executorId, hostname, port)
    master.registerExecutorStatsCollector(executorStatsCollectorId, slaveEndpoint)
  }

  private def hello() {
    val sc = SparkContext.getOrCreate()

  }
}

private[spark] object ExecutorStatsCollector {
  private val ID_GENERATOR = new IdGenerator

}
