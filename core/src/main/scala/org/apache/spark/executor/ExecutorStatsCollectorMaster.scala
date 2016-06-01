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

import org.apache.spark.executor.ExecutorStatsMessages.RegisterExecutorStatsCollector
import org.apache.spark.{SparkException, SparkConf}
import org.apache.spark.internal.Logging
import org.apache.spark.rpc.RpcEndpointRef
import org.apache.spark.util.RpcUtils

class ExecutorStatsCollectorMaster(
  var driverEndpoint: RpcEndpointRef,
  conf: SparkConf,
  isDriver: Boolean) extends Logging {
  val timeout = RpcUtils.askRpcTimeout(conf)

  def registerExecutorStatsCollector
  (executorStatsCollectorId: ExecutorStatsCollectorId,
   slaveEndpoint: RpcEndpointRef) : Unit = {
    logInfo(s"Registering ExecutorStatsCollector $executorStatsCollectorId")
    tell(RegisterExecutorStatsCollector(executorStatsCollectorId, slaveEndpoint))
    logInfo(s"Registered ExecutorStatsCollector $executorStatsCollectorId")
  }
  private def tell(message: Any) {
    if (!driverEndpoint.askWithRetry[Boolean](message)) {
      throw new SparkException("ExecutorStatsCollectorMaster returned false, expected true.")
    }
  }
}

object ExecutorStatsCollectorMaster {
  val DRIVER_ENDPOINT_NAME = "ExecutorStatsCollectorMaster"
}
