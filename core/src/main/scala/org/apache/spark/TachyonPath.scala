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
package org.apache.spark

import tachyon.client.file.TachyonFileSystem
import tachyon.client.file.TachyonFileSystem.TachyonFileSystemFactory
import org.apache.spark.util.Utils

object TachyonPath {
  val tfs: TachyonFileSystem = TachyonFileSystemFactory.get()
  var folder = ""

  def setFolder(): Unit = {
    folder = sys.env.get("APP_NAME").getOrElse("LiveJournalPageRank").split('.').last
//    folder = System.getProperty("spark.app.name").split('.').last
    if (folder.length != 0) {
      folder += "_"
    }
  }

  def rddDependency: String = {
    setFolder()
    "/" + folder + "rddDependency"
  }

  def candidateRdds: String = {
    setFolder()
    "/" + folder + "candidateRdds"
  }

  def stageRDDs: String = {
    setFolder()
    "/" + folder + "stageRDDs"
  }

  def numOfRecords(samplingRate: Double): String = {
    setFolder()
    "/" + folder + samplingRate + "_numOfRecords"
  }

  def blockSelectivity(samplingRate: Double): String = {
    setFolder()
    "/" + folder + samplingRate + "_blockSelectivity"
  }

  def taskDescription: String = {
    setFolder()
    "/" + folder + "taskDescription"
  }

  def trainingData(): List[String] = {
    setFolder()
    Utils.getTachyonPaths(folder)
  }

  def rddPrediction: String = {
    // From TabuSearch
    setFolder()
    "/" + folder + "rddPrediction"
  }

  def executorIdToParDep: String = {
    setFolder()
    "/" + folder + "executorIdToParDep"
  }

  def rddResult: String = {
    // From MyTS
    setFolder()
    "/" + folder + "rddResult"
  }

  def broadcastBlock: String = {
    setFolder()
    "/" + folder + "broadcastBlock"
  }

  def exeuctionMemory: String = {
    setFolder()
    "/" + folder + "executionMemory"
  }

  def storageLevel: String = {
    setFolder()
    "/" + folder + "storageLevel"
  }
}
