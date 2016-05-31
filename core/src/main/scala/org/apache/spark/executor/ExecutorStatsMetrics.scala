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

// Stats for each executor (machine / worker) from Ganglia

class ExecutorStatsMetrics private[spark]() extends Serializable {
  // CPU
  // Percentage of CPU utilization that occurred while executing at the system level
  private var _cpuSystem: Float = _
  // It is the number of threads (kernel level) that are runnable and queued
  // while waiting for CPU resources, averaged over one minute.
  private var _loadOne: Float = _
  // Five minute load average
  private var _loadFive: Float = _
  // Fifteen minute load average
  private var _loadFifteen: Float = _
  // Percentage of CPU cycles spent idle since last boot (Linux)
  private var _cpuAIdle: Float = _
  // Percentage of CPU cycles spent waiting for I/O (Solaris)
  private var _cpuWIO: Float = _
  // Percentage of CPU cycles spent in user mode
  private var _cpuUser: Float = _
  // Percentage of time that the CPU or CPUs were idle and
  // the system did not have an outstanding disk I/O request
  private var _cpuIdle: Float = _

  // Memory
  // Amount of memory allocated to system buffers (Linux)
  private var _memBuffers: Float = _
  // Amount of memory allocated to cached data (Linux)
  private var _memCached: Float = _
  // Amount of available memory
  private var _memFree: Float = _
  // Amount of free swap space
  private var _swapFree: Float = _
  // Amount of total swap space
  private var _swapTotal: Float = _

  // Setters
  // CPU
  private[spark] def setCpuSystem(v: Float): Unit = _cpuSystem = v
  private[spark] def setLoadOne(v: Float): Unit = _loadOne = v
  private[spark] def setLoadFive(v: Float): Unit = _loadFive = v
  private[spark] def setLoadFifteen(v: Float): Unit = _loadFifteen = v
  private[spark] def setCpuAIdle(v: Float): Unit = _cpuAIdle = v
  private[spark] def setCpuWIO(v: Float): Unit = _cpuWIO = v
  private[spark] def setCpuUser(v: Float): Unit = _cpuUser = v
  private[spark] def setCpuIdle(v: Float): Unit = _cpuIdle = v

  // Memory
  private[spark] def setMemBuffers(v: Float): Unit = _memBuffers = v
  private[spark] def setMemCached(v: Float): Unit = _memCached = v
  private[spark] def setMemFree(v: Float): Unit = _memFree = v
  private[spark] def setSwapFree(v: Float): Unit = _swapFree = v
  private[spark] def setSwapTotal(v: Float): Unit = _swapTotal = v

  // Getters
  // CPU
  def cpuSystem: Float = _cpuSystem
  def loadOne: Float = _loadOne
  def loadFive: Float = _loadFive
  def loadFifteen: Float = _loadFifteen
  def cpuAIdle: Float = _cpuAIdle
  def cpuWIO: Float = _cpuWIO
  def cpuUser: Float = _cpuUser
  def cpuIdle: Float = _cpuIdle

  // Memory
  def memBuffers: Float = _memBuffers
  def memCached: Float = _memCached
  def memFree: Float = _memFree
  def swapFree: Float = _swapFree
  def swapTotal: Float = _swapTotal


}

object ExecutorStatsMetrics {
  def empty: ExecutorStatsMetrics = {
    val esm = new ExecutorStatsMetrics
    esm
  }
}
