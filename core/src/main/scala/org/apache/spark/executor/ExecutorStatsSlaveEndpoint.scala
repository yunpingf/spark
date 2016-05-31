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

import org.apache.spark.MapOutputTracker
import org.apache.spark.executor.ExecutorStatsMessages.HelloFromMaster
import org.apache.spark.internal.Logging
import org.apache.spark.rpc.{RpcCallContext, RpcEnv, ThreadSafeRpcEndpoint}
import org.apache.spark.util.ThreadUtils

import scala.concurrent.{ExecutionContext, Future}

class ExecutorStatsSlaveEndpoint
(
  override val rpcEnv: RpcEnv,
  executorStatsCollector: ExecutorStatsCollector,
  mapOutputTracker: MapOutputTracker
) extends ThreadSafeRpcEndpoint with Logging {
  private val asyncThreadPool =
    ThreadUtils.newDaemonCachedThreadPool("executor-stats-slave-async-thread-pool")
  private implicit val asyncExecutionContext = ExecutionContext.fromExecutorService(asyncThreadPool)

  override def receiveAndReply(context: RpcCallContext): PartialFunction[Any, Unit] = {
    case HelloFromMaster(message: String) =>
      doAsync[Boolean]("Recieved from Master: " + message, context) {
        true
      }
  }

  private def doAsync[T](actionMessage: String, context: RpcCallContext)(body: => T) {
    val future = Future {
      logDebug(actionMessage)
      body
    }
    future.onSuccess { case response =>
      logDebug("Done " + actionMessage + ", response is " + response)
      context.reply(response)
      logDebug("Sent response: " + response + " to " + context.senderAddress)
    }
    future.onFailure { case t: Throwable =>
      logError("Error in " + actionMessage, t)
      context.sendFailure(t)
    }
  }

  override def onStop(): Unit = {
    asyncThreadPool.shutdownNow()
  }
}
