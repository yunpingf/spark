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

import org.apache.spark.SparkConf
import org.apache.spark.executor.ExecutorStatsMessages.{RegisterExecutorStatsCollector, HelloFromSlave}
import org.apache.spark.internal.Logging
import org.apache.spark.rpc.{RpcEndpointRef, RpcCallContext, ThreadSafeRpcEndpoint, RpcEnv}
import org.apache.spark.scheduler._
import org.apache.spark.storage.BlockManagerId
import org.apache.spark.util.{Utils, ThreadUtils}

import scala.collection.mutable
import scala.concurrent.ExecutionContext

class ExecutorStatsMasterEndpoint(
  override val rpcEnv: RpcEnv,
  val isLocal: Boolean,
  conf: SparkConf,
  listenerBus: LiveListenerBus
) extends ThreadSafeRpcEndpoint with Logging {
  private val executorStatsCollectorInfo =
    new mutable.HashMap[ExecutorStatsCollectorId, ExecutorStatsCollectorInfo]

  private val executorStatsCollectorIdByExecutor =
    new mutable.HashMap[String, ExecutorStatsCollectorId]

  private val askThreadPool =
    ThreadUtils.newDaemonCachedThreadPool("executor-stats-master-ask-thread-pool")

  private implicit val askExecutionContext = ExecutionContext.fromExecutorService(askThreadPool)

  override def receiveAndReply(context: RpcCallContext): PartialFunction[Any, Unit] = {
    case HelloFromSlave(message: String) => context.reply(true)
    case RegisterExecutorStatsCollector(executorStatsCollectorId, slaveEndpoint) =>
      register(executorStatsCollectorId, slaveEndpoint)
      context.reply(true)
  }

  private def register(id: ExecutorStatsCollectorId, slaveEndpoint: RpcEndpointRef) = {
    val time = System.currentTimeMillis()
    if (!executorStatsCollectorInfo.contains(id)) {
      logInfo("Registering block manager %s with %s".format(
        id.hostPort, id))

      executorStatsCollectorIdByExecutor(id.executorId) = id
      executorStatsCollectorInfo(id) = new ExecutorStatsCollectorInfo(
        id, System.currentTimeMillis(), slaveEndpoint)
    }
    listenerBus.post(SparkListenerExecutorStatsCollectorAdded(time, id))
  }
}

private[spark] class ExecutorStatsCollectorInfo(
  val executorStatsCollectorId: ExecutorStatsCollectorId,
  timeMs: Long,
  val slaveEndpoint: RpcEndpointRef) extends Logging {
  private var _lastSeenMs: Long = timeMs
  def updateLastSeenMs() {
    _lastSeenMs = System.currentTimeMillis()
  }
}
