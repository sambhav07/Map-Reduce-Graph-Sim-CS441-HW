package com.lsc

import GraphUtils.{loadGraphAndGenerateActionCombinations, loadGraphAndGenerateCombinations}

import NetGraphAlgebraDefs.GraphPerturbationAlgebra.ModificationRecord
import NetGraphAlgebraDefs.NetModelAlgebra.{actionType, outputDirectory}
import NetGraphAlgebraDefs.*
import NetModelAnalyzer.Analyzer
import Randomizer.SupplierOfRandomness
import Utilz.{CreateLogger, NGSConstants}
import com.google.common.graph.ValueGraph
import com.typesafe.config.ConfigFactory
import guru.nidi.graphviz.engine.Format
import org.slf4j.Logger

import java.io.*
import java.net.{InetAddress, NetworkInterface, Socket}
import java.util.concurrent.{ThreadLocalRandom, TimeUnit}
import scala.concurrent.duration.*
import scala.concurrent.{Await, ExecutionContext, Future}
import scala.jdk.CollectionConverters.*
import scala.util.matching.Regex
import scala.util.{Failure, Success}
import software.amazon.awssdk.regions.Region
import software.amazon.awssdk.services.s3.S3Client
import software.amazon.awssdk.services.s3.model.GetObjectRequest
import java.nio.file.Paths


object MyMain:
  val logger:Logger = CreateLogger(classOf[MyMain.type])
  val ipAddr: InetAddress = InetAddress.getLocalHost
  val hostName: String = ipAddr.getHostName
  val hostAddress: String = ipAddr.getHostAddress


  def parsePath(  path: String): (String, String, Boolean) = {
    logger.trace(s"Parsing path: $path")
    if (path.startsWith("s3://") || path.startsWith("https://")) {
      val isRemote = true
      val directory = if (path.startsWith("s3://")) {
        val withoutPrefix = path.drop("s3://".length)
        val slashIndex = withoutPrefix.indexOf('/')
        "s3://" + withoutPrefix.take(slashIndex)
      } else {
        val url = new java.net.URL(path)
        url.getProtocol + "://" + url.getHost + url.getPath.take(url.getPath.lastIndexOf('/'))
      }
      val fileName = path.drop(directory.length + 1)
      (directory, fileName, isRemote)
    } else if (new File(path).exists()) { // Check if it's a local file path
      val isRemote = false
      val filePath = Paths.get(path)
      val directory = filePath.getParent.toString
      val fileName = filePath.getFileName.toString
      (directory, fileName, isRemote)
    } else {
      logger.warn(s"Unsupported path format: $path")
      throw new IllegalArgumentException(s"Unsupported path format: $path")
    }
  }
  def main(args: Array[String]): Unit =
    logger.info("Main method started")
    val (directory1, fileName1, isRemote1) = parsePath(args(0))
    val (_, fileName2, isRemote2) = parsePath(args(1)) // Assuming both files are in the same directory

    loadGraphAndGenerateCombinations(fileName1, fileName2, directory1, isRemote1, isRemote2)
    loadGraphAndGenerateActionCombinations(fileName1, fileName2, isRemote1, isRemote2, directory1)

    logger.info(s"Starting the main class for running all jobs together")

    logger.info(s"Running NodeSimilarityScore Job")
//    val nodeJobStatus = NodeSimilarityScore.runMapReduceNodes(args(2), args(3)) //Functionality 1

    val isEMR = directory1.startsWith("s3://") || directory1.startsWith("http://") || directory1.startsWith("https://")

    val nodeOutputPath = if (isEMR) s"${args(2)}/output_nodes" else args(3)

    val nodeJobStatus = NodeSimilarityScore.runMapReduceNodes(s"$directory1/Combinations.txt", nodeOutputPath) //Functionality 1


    val edgeOutputPath = if (isEMR) s"${args(2)}/output_edges" else args(5)
    logger.info(s"Running EdgeSimilarityScore")
    val edgeJobStatus = EdgeSimilarityScore.runMapReduceEdges(s"$directory1/ActionCombinations.txt", edgeOutputPath) //Functionality 2


    val (resultPath1, resultPath2, anotherOutputPath) = if (isEMR) {
      // If it's EMR, use the S3 paths or HTTP/HTTPS paths
      (nodeOutputPath, edgeOutputPath,s"${args(2)}/output_results")
    } else {
      // If it's local, use the local file paths
      (args(3), args(5), args(6))
    }

    if(nodeJobStatus && edgeJobStatus){
      logger.info("Both node and edge jobs were successful")
      logger.info(s"Running NodeEdgesResult")
      val resultJobStatus = NodeEdgesResult.runMapReduceResult(resultPath1, resultPath2, anotherOutputPath) //Functionality 3
      if(resultJobStatus){
        logger.info("Result job was successful")
        val (firstArg, secondArg) = if (isEMR) {
          // If it's EMR, use the S3 paths or HTTP/HTTPS paths
          (args(2) + "/output_result", directory1 + "/goldenSet.yaml")
        } else {
          // If it's local, use the local file paths
          (args(7), args(8))
        }
        CombinedParser.main(Array(firstArg, secondArg))
      }else{
        logger.warn("Result job failed") // Added warn log
      }
    }
    logger.info("Main method ended") // Added info log

