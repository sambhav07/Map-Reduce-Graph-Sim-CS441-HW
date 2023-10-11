package com.lsc

import NetGraphAlgebraDefs.*

import scala.jdk.CollectionConverters.*
import java.io.{BufferedWriter, FileWriter}
import java.nio.file.{Files, Paths}
import java.nio.charset.StandardCharsets
import scala.collection.mutable
import software.amazon.awssdk.services.s3.S3Client
import software.amazon.awssdk.services.s3.model.PutObjectRequest
import java.nio.ByteBuffer
import software.amazon.awssdk.core.sync.RequestBody
import java.net.URL

object GraphUtils {
  // Define your utility functions here, e.g., generateAdjacencyMatrix and writeAdjacencyMatrixToFile
  // ...

//  val outputDirectory = "/Users/sambhavjain/Desktop/cs441-assignments/NetGameSim/outputs"

//  def generateAdjacencyMatrix(nodes: Set[NodeObject], actions: Set[Action]): Array[Array[Double]] = {
//    val nodeSize: Int = nodes.size
//    val adjacencyMatrix = Array.ofDim[Double](nodeSize, nodeSize)
//
//    // Iterate over the nodes set
//    nodes.foreach { node =>
//      // Extract the first value from the NodeObject
//      val firstValue = node.id
//
//      // Calculate the value to store in the matrix cell [firstValue][firstValue]
//      val valueToStore = node.children + node.props + node.currentDepth + node.propValueRange +
//        node.maxDepth + node.maxBranchingFactor + node.maxProperties + node.storedValue
//
//      // Store the value in the matrix cell
//      adjacencyMatrix(firstValue)(firstValue) = valueToStore
//    }
//
//    // Iterate over each action
//    actions.foreach { action =>
//      // Extract source and destination nodes
//      val sourceNode = action.fromNode
//      val destinationNode = action.toNode
//
//      // Extract relevant properties and calculate the properties sum
//      val propertiesSum = action.actionType + action.fromId + action.toId +
//        action.resultingValue.getOrElse(0) + action.cost.toFloat
//
//      // Update the adjacency matrix cell
//      if (sourceNode.id >= 0 && sourceNode.id < nodeSize &&
//        destinationNode.id >= 0 && destinationNode.id < nodeSize) {
//        // Store properties sum in the adjacency matrix cell [sourceNode.id][destinationNode.id]
//        adjacencyMatrix(sourceNode.id)(destinationNode.id) = propertiesSum
//      }
//    }
//
//    adjacencyMatrix
//  }


//  def writeAdjacencyMatrixToFile(adjacencyMatrix: Array[Array[Double]], fileName: String): Unit = {
//    // Create a FileWriter to write to the file
//    val fileWriter = new FileWriter(fileName)
//
//    try {
//      // Create a BufferedWriter to efficiently write to the file
//      val bufferedWriter = new BufferedWriter(fileWriter)
//
//      // Iterate over the rows and columns of the adjacency matrix
//      for (row <- adjacencyMatrix) {
//        // Join the values in the row with tabs and write to the file
//        val rowString = row.map(value => f"$value%.9f").mkString("\t")
//        bufferedWriter.write(rowString)
//        bufferedWriter.newLine() // Move to the next line
//      }
//
//      // Close the BufferedWriter to flush and close the file
//      bufferedWriter.close()
//    } finally {
//      // Close the FileWriter in a finally block to ensure it's closed even if an exception occurs
//      fileWriter.close()
//    }
//  }

//  def loadGraphAndExtractData(graphFileName: String, outputDirectory: String): (Set[NodeObject], Set[Action]) = {
//    val graph = NetGraph.load(graphFileName, outputDirectory, isRemoteOriginal, isRemotePerturbed)
//    val nodes: Set[NodeObject] = graph match {
//      case Some(g) => g.sm.nodes().asScala.toSet
//      case None => Set.empty[NodeObject]
//    }
//    val actions: Set[Action] = graph match {
//      case Some(g) => g.getActions
//      case None => Set.empty[Action]
//    }
//
//    // Extract the "perturbed" keyword from the file name if present
//    val suffix = if (graphFileName.contains("perturbed")) "-perturbed" else ""
//
//    // Write nodes to NodesList with suffix to distinguish between original and perturbed
//    val nodesContent = nodes.map(_.toString).mkString("\n")
//    val nodesFilePath = Paths.get(s"$outputDirectory/NodesList$suffix.txt")
//    Files.write(nodesFilePath, nodesContent.getBytes(StandardCharsets.UTF_8))
//
//    // Write actions to ActionsList with suffix to distinguish between original and perturbed
//    val actionsContent = actions.map(_.toString).mkString("\n")
//    val actionsFilePath = Paths.get(s"$outputDirectory/ActionsList$suffix.txt")
//    Files.write(actionsFilePath, actionsContent.getBytes(StandardCharsets.UTF_8))
//
//    (nodes, actions)
//  }


  def writeToPath(content: String, path: String): Unit = {
    if (path.startsWith("s3://")) {
      val s3Client = S3Client.create()

      val bucketAndKey = path.drop("s3://".length).split("/", 2)
      val bucket = bucketAndKey(0)
      val key = bucketAndKey(1)

      val requestBody = RequestBody.fromByteBuffer(ByteBuffer.wrap(content.getBytes(StandardCharsets.UTF_8)))

      val putObjectRequest = PutObjectRequest.builder()
        .bucket(bucket)
        .key(key)
        .build()

      s3Client.putObject(putObjectRequest, requestBody)
    } else if (path.startsWith("https://")) {
      val url = new URL(path)
      val host = url.getHost
      val bucket = host.substring(0, host.indexOf("."))
      val key = url.getPath.drop(1) // Drop the leading '/'

      val s3Client = S3Client.create()
      val requestBody = RequestBody.fromByteBuffer(ByteBuffer.wrap(content.getBytes(StandardCharsets.UTF_8)))

      val putObjectRequest = PutObjectRequest.builder()
        .bucket(bucket)
        .key(key)
        .build()

      s3Client.putObject(putObjectRequest, requestBody)
    } else {
      Files.write(Paths.get(path), content.getBytes(StandardCharsets.UTF_8))
    }
  }
  def loadGraphAndGenerateCombinations(originalGraphFileName: String, perturbedGraphFileName: String, outputDirectory: String, isRemoteOriginal: Boolean,
                                       isRemotePerturbed: Boolean): Unit = {
    val originalGraph = NetGraph.load(originalGraphFileName, outputDirectory, isRemoteOriginal, isRemotePerturbed)
    val perturbedGraph = NetGraph.load(perturbedGraphFileName, outputDirectory, isRemoteOriginal, isRemotePerturbed)

    // Using a single map to hold both min and max values with attribute as the key
    val attributeRanges = mutable.Map[String, (Double, Double)]()

    (originalGraph, perturbedGraph) match {
      case (Some(original), Some(perturbed)) =>
        val originalNodes = original.sm.nodes().asScala.toSet
        val perturbedNodes = perturbed.sm.nodes().asScala.toSet

        val combinations = for {
          originalNode <- originalNodes
          perturbedNode <- perturbedNodes
        } yield {
          // Update attribute ranges
          updateNodeAttributeRanges(originalNode, attributeRanges)
          updateNodeAttributeRanges(perturbedNode, attributeRanges)
          s"$originalNode $perturbedNode"
        }
        val combinationsContent = combinations.mkString("\n")
        writeToPath(combinationsContent, s"$outputDirectory/Combinations.txt")

        // Write attributeRanges to file
        val rangesContent = attributeRanges.map {
          case (key, (min, max)) => s"$key: $min to $max"
        }.mkString("\n")

        writeToPath(rangesContent, s"$outputDirectory/AttributeRanges.txt")

        attributeRanges.toMap // Convert mutable map to immutable

      case _ =>
        println("Could not load one or both graphs.")
        Map.empty[String, (Double, Double)]
    }
  }

  def updateNodeAttributeRanges(node: NodeObject, attributeRanges: mutable.Map[String, (Double, Double)]): Unit = {
    val attributes = Map(
      "children" -> node.children.toDouble,
      "props" -> node.props.toDouble,
      "currentDepth" -> node.currentDepth.toDouble,
      "propValueRange" -> node.propValueRange.toDouble,
      "maxDepth" -> node.maxDepth.toDouble,
      "maxBranchingFactor" -> node.maxBranchingFactor.toDouble,
      "maxProperties" -> node.maxProperties.toDouble,
      "storedValue" -> node.storedValue
    )

    for ((attrName, attrValue) <- attributes) {
      attributeRanges.get(attrName) match {
        case Some((min, max)) =>
          attributeRanges(attrName) = (math.min(min, attrValue), math.max(max, attrValue))
        case None =>
          attributeRanges(attrName) = (attrValue, attrValue)
      }
    }
  }

  import scala.jdk.CollectionConverters._

  def loadGraphAndGenerateActionCombinations(originalGraphFileName: String, perturbedGraphFileName: String, isRemoteOriginal: Boolean, isRemotePerturbed: Boolean, outputDirectory: String): Unit = {
    val originalGraph = NetGraph.load(originalGraphFileName, outputDirectory, isRemoteOriginal, isRemotePerturbed)
    val perturbedGraph = NetGraph.load(perturbedGraphFileName, outputDirectory, isRemoteOriginal, isRemotePerturbed)

    // Using a single map to hold both min and max values with attribute as the key
    val attributeRanges = mutable.Map[String, (Double, Double)]()

    (originalGraph, perturbedGraph) match {
      case (Some(original), Some(perturbed)) =>
        val originalActions = original.getActions
        val perturbedActions = perturbed.getActions

        val combinations = for {
          originalAction <- originalActions
          perturbedAction <- perturbedActions
        } yield {
          // Update attribute ranges - You might want to modify this part based on how you want to deal with actions
          updateActionAttributeRanges(originalAction, attributeRanges)
          updateActionAttributeRanges(perturbedAction, attributeRanges)
          s"$originalAction $perturbedAction"
        }

        val combinationsContent = combinations.mkString("\n")
//        Files.write(Paths.get(s"$outputDirectory/ActionCombinations.txt"), combinationsContent.getBytes(StandardCharsets.UTF_8))
        writeToPath(combinationsContent, s"$outputDirectory/ActionCombinations.txt")

        // Write attributeRanges to file
        val rangesContent = attributeRanges.map {
          case (key, (min, max)) => s"$key: $min to $max"
        }.mkString("\n")

//        Files.write(Paths.get(s"$outputDirectory/ActionAttributeRanges.txt"), rangesContent.getBytes(StandardCharsets.UTF_8))
        writeToPath(rangesContent, s"$outputDirectory/ActionAttributeRanges.txt")

        attributeRanges.toMap // Convert mutable map to immutable

      case _ =>
        println("Could not load one or both graphs.")
        Map.empty[String, (Double, Double)]
    }
  }

  // The updateAttributeRanges function would need to be modified to update the ranges based on Action attributes
  def updateActionAttributeRanges(action: Action, attributeRanges: mutable.Map[String, (Double, Double)]): Unit = {
    // List of attribute names and their corresponding values
    val attributes = List(
      ("actionType", action.actionType.toDouble),
      ("fromId", action.fromId.toDouble),
      ("toId", action.toId.toDouble),
      ("cost", action.cost)
    ) ++ action.resultingValue.map(v => ("resultingValue", v.toDouble))  // Handle optional attribute

    // Iterate through each attribute and update the range in attributeRanges map
    attributes.foreach { case (name, value) =>
      attributeRanges.get(name) match {
        case Some((min, max)) =>
          attributeRanges(name) = (Math.min(min, value), Math.max(max, value))
        case None =>
          attributeRanges(name) = (value, value)
      }
    }
  }

}
