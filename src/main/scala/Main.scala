package com.lsc

//import GraphUtils.{loadGraphAndGenerateCombinations, loadGraphAndGenerateActionCombinations}

import NetGraphAlgebraDefs.GraphPerturbationAlgebra.ModificationRecord
import NetGraphAlgebraDefs.NetModelAlgebra.{actionType, outputDirectory}
import NetGraphAlgebraDefs.{Action, GraphPerturbationAlgebra, NetGraph, NetModelAlgebra, NodeObject}
import NetModelAnalyzer.Analyzer
import Randomizer.SupplierOfRandomness
import Utilz.{CreateLogger, NGSConstants}
import com.google.common.graph.ValueGraph

import java.util.concurrent.{ThreadLocalRandom, TimeUnit}
import scala.concurrent.duration.*
import scala.concurrent.{Await, ExecutionContext, Future}
import com.typesafe.config.ConfigFactory
import guru.nidi.graphviz.engine.Format
import org.slf4j.Logger

import java.net.{InetAddress, NetworkInterface, Socket}
import scala.util.{Failure, Success}
import scala.util.matching.Regex
import java.io.*

object Main:
  val logger:Logger = CreateLogger(classOf[Main.type])
  val ipAddr: InetAddress = InetAddress.getLocalHost
  val hostName: String = ipAddr.getHostName
  val hostAddress: String = ipAddr.getHostAddress

  def main(args: Array[String]): Unit =
    import scala.jdk.CollectionConverters.*
    val outGraphFileName = if args.isEmpty then NGSConstants.OUTPUTFILENAME else args(0).concat(NGSConstants.DEFOUTFILEEXT)
    val perturbedOutGraphFileName = outGraphFileName.concat(".perturbed")
    logger.info(s"Output graph file is $outputDirectory$outGraphFileName and its perturbed counterpart is $outputDirectory$perturbedOutGraphFileName")
    logger.info(s"The netgraphsim program is run at the host $hostName with the following IP addresses:")
    logger.info(ipAddr.getHostAddress)
    NetworkInterface.getNetworkInterfaces.asScala
        .flatMap(_.getInetAddresses.asScala)
        .filterNot(_.getHostAddress == ipAddr.getHostAddress)
        .filterNot(_.getHostAddress == "127.0.0.1")
        .filterNot(_.getHostAddress.contains(":"))
        .map(_.getHostAddress).toList.foreach(a => logger.info(a))

    val existingGraph = java.io.File(s"$outputDirectory$outGraphFileName").exists
    val g: Option[NetGraph] = if existingGraph then
      logger.warn(s"File $outputDirectory$outGraphFileName is located, loading it up. If you want a new generated graph please delete the existing file or change the file name.")
      NetGraph.load(fileName = s"$outputDirectory$outGraphFileName")
    else
      val config = ConfigFactory.load()
      logger.info("for the main entry")
      config.getConfig("NGSimulator").entrySet().forEach(e => logger.info(s"key: ${e.getKey} value: ${e.getValue.unwrapped()}"))
      logger.info("for the NetModel entry")
      config.getConfig("NGSimulator").getConfig("NetModel").entrySet().forEach(e => logger.info(s"key: ${e.getKey} value: ${e.getValue.unwrapped()}"))
      NetModelAlgebra()

    if g.isEmpty then logger.error("Failed to generate a graph. Exiting...")
    else
      logger.info(s"The original graph contains ${g.get.totalNodes} nodes and ${g.get.sm.edges().size()} edges; the configuration parameter specified ${NetModelAlgebra.statesTotal} nodes.")
      if !existingGraph then
        g.get.persist(fileName = outGraphFileName)
        logger.info(s"Generating DOT file for graph with ${g.get.totalNodes} nodes for visualization as $outputDirectory$outGraphFileName.dot")
        g.get.toDotVizFormat(name = s"Net Graph with ${g.get.totalNodes} nodes", dir = outputDirectory, fileName = outGraphFileName, outputImageFormat = Format.DOT)
        logger.info(s"A graph image file can be generated using the following command: sfdp -x -Goverlap=scale -Tpng $outputDirectory$outGraphFileName.dot > $outputDirectory$outGraphFileName.png")
      end if
      logger.info("Perturbing the original graph to create its modified counterpart...")
      val perturbation: GraphPerturbationAlgebra#GraphPerturbationTuple = GraphPerturbationAlgebra(g.get.copy)
      perturbation._1.persist(fileName = perturbedOutGraphFileName)

      logger.info(s"Generating DOT file for graph with ${perturbation._1.totalNodes} nodes for visualization as $outputDirectory$perturbedOutGraphFileName.dot")
      perturbation._1.toDotVizFormat(name = s"Perturbed Net Graph with ${perturbation._1.totalNodes} nodes", dir = outputDirectory, fileName = perturbedOutGraphFileName, outputImageFormat = Format.DOT)
      logger.info(s"A graph image file for the perturbed graph can be generated using the following command: sfdp -x -Goverlap=scale -Tpng $outputDirectory$perturbedOutGraphFileName.dot > $outputDirectory$perturbedOutGraphFileName.png")

      val modifications:ModificationRecord = perturbation._2
      GraphPerturbationAlgebra.persist(modifications, outputDirectory.concat(outGraphFileName.concat(".yaml"))) match
        case Left(value) => logger.error(s"Failed to save modifications in ${outputDirectory.concat(outGraphFileName.concat(".yaml"))} for reason $value")
        case Right(value) =>
          logger.info(s"Diff yaml file ${outputDirectory.concat(outGraphFileName.concat(".yaml"))} contains the delta between the original and the perturbed graphs.")
          logger.info(s"Done! Please check the content of the output directory $outputDirectory")

//
//      val originalGraph = NetGraph.load("NetGraph_17-09-23-16-45-50.ngs", outputDirectory)
//      println(originalGraph.toString)
////
//      val perturbedGraph = NetGraph.load("NetGraph_17-09-23-16-45-50.ngs.perturbed", outputDirectory)
//      println(perturbedGraph.toString)

//      println("originalGraph"+ originalGraph)
//      println("perturbedGraph"+ perturbedGraph)
       /// loadGraphAndGenerateCombinations("NetGraph_09-10-23-01-05-17.ngs", "NetGraph_09-10-23-01-05-17.ngs.perturbed", outputDirectory)
//      val (nodesPerturbed, actionsPerturbed) = loadGraphAndExtractData("NetGraph_17-09-23-16-45-50.ngs.perturbed", outputDirectory)
//        val originalGraph=NetGraph.load("NetGraph_17-09-23-16-45-50.ngs", outputDirectory)
//        println(originalGraph)

        ///loadGraphAndGenerateActionCombinations("NetGraph_09-10-23-01-05-17.ngs", "NetGraph_09-10-23-01-05-17.ngs.perturbed", outputDirectory)


//
//      val actions: Set[Action] = originalGraph match {
//        case Some(graph) => graph.getActions
//        case None => Set.empty[Action]
//      }

      // Generate the adjacency matrix
//      val adjacencyMatrixOriginal = generateAdjacencyMatrix(nodesOriginal, actionsOriginal)
//      val adjacencyMatrixPerturbed = generateAdjacencyMatrix(nodesPerturbed, actionsPerturbed)
//
//      // Print the adjacency matrix
//      println("Adjacency Matrix Original:")
//      adjacencyMatrixOriginal.foreach(row => println(row.mkString("\t")))
//
//      println("Adjacency Matrix Perturbed:")
//      adjacencyMatrixPerturbed.foreach(row => println(row.mkString("\t")))
//
//      // Write the adjacency matrix to a file
//      val fileNameOriginal = "outputs/original_adjacency_matrix.txt"
//      writeAdjacencyMatrixToFile(adjacencyMatrixOriginal, fileNameOriginal)
//
//      val fileNamePerturbed = "outputs/perturbed_adjacency_matrix.txt"
//      writeAdjacencyMatrixToFile(adjacencyMatrixPerturbed, fileNamePerturbed)
//

//      // Assuming you have already loaded the NetGraph into `graph2`
//
//      // Assuming you have graph2 as an Option[NetGraph]
//      val nodes: Set[NodeObject] = originalGraph match {
//        case Some(graph) => graph.sm.nodes().asScala.toSet
//        case None => Set.empty[NodeObject]
//      }
//
//
//      val nodeSize: Int = nodes.size
//      println("Nodes Set:")
//      nodes.foreach(println)
//
//
//
//      println(s"Number of Nodes: $nodeSize")
//
//    // Define the adjacency matrix
//    val adjacencyMatrix = Array.ofDim[Double](nodeSize, nodeSize)
//
//    // Iterate over the nodes set
//    nodes.foreach { node =>
//      // Extract the first value from the NodeObject
//      val firstValue = node.id
//
//      // Calculate the value to store in the matrix cell [firstValue][firstValue]
//      // Calculate the value to store in the matrix cell [firstValue][firstValue]
//      val valueToStore = node.children + node.props + node.currentDepth + node.propValueRange + node.maxDepth + node.maxBranchingFactor + node.maxProperties + node.storedValue
//
//      // Store the value in the matrix cell
//      adjacencyMatrix(firstValue)(firstValue) = valueToStore
//    }
//
//    // Print the adjacency matrix
//    println("Adjacency Matrix:")
//    adjacencyMatrix.foreach(row => println(row.mkString("\t")))
//
//    val actions: Set[Action] = originalGraph match {
//      case Some(graph) => graph.getActions
//      case None => Set.empty[Action]
//    }
//
//    // Now you have a set of actions from the graph2 object
//    println(actions)
//
//// Iterate over each action
//    for (action <- actions) {
//      // Extract source and destination nodes
//      val sourceNode = action.fromNode
//      val destinationNode = action.toNode
//
//      // Extract relevant properties and calculate the properties sum
//      val propertiesSum = action.actionType + action.fromId + action.toId + action.resultingValue.getOrElse(0) + action.cost.toFloat
//
//      // Update the adjacency matrix cell
//      if (sourceNode.id >= 0 && sourceNode.id < adjacencyMatrix.length &&
//        destinationNode.id >= 0 && destinationNode.id < adjacencyMatrix.length) {
//        // Store properties sum in the adjacency matrix cell [sourceNode.id][destinationNode.id]
//        adjacencyMatrix(sourceNode.id)(destinationNode.id) = propertiesSum
//      }
//    }
//
//    // Print the adjacency matrix
//    println("Adjacency Matrix:")
//    adjacencyMatrix.foreach(row => println(row.mkString("\t")))
//
//    // Assuming you have the adjacency matrix as adjacencyMatrix and a file name
//    val fileName = "outputs/original_adjacency_matrix.txt"
//
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

// The adjacency matrix is now stored in the "adjacency_matrix.txt" file.
//    val graphOption: Option[NetGraphAlgebraDefs.NetGraph] = graph2// Your Option[NetGraph] instance
//
//    // Unwrap the Option and provide a default value if it's None
//    val graph: NetGraphAlgebraDefs.NetGraph = graphOption.getOrElse(graph2)
//
//    // Access the nodes and print them
//    val nodes2 = graph2.sm.nodes().asScala.toList
//    println("Nodes:")
//    nodes2.foreach(node => println(node))
      //      // Now, you can iterate over the nodes or perform operations on them
//      for (node <- nodes) {
//        println(s"Node ID: ${node.id}, In-Degree: ${graph2.sm.inDegree(node)}, Out-Degree: ${graph2.sm.outDegree(node)}")
//      }
//
//      // You can also access specific nodes by their ID if needed
//      val nodeIdToFind: Int = 1 // Replace with the ID you want to find
//      val nodeToFind: Option[NodeObject] = nodes.find(_.id == nodeIdToFind)
//      nodeToFind.foreach(node => println(s"Found Node ID $nodeIdToFind: $node"))
//

//      // Define the regex pattern to match Action objects
//      val actionPattern: Regex = """Action\(\d+,(NodeObject\([^)]+\)),(NodeObject\([^)]+\)),\d+,\d+,(?:Some\(\d+\)|None),(\d+\.\d+)\)""".r
//
//      // Extract Action objects from the data
//      val actionObjects: Seq[String] = actionPattern.findAllIn(graph2.toString).toList
//
//      // Print the extracted Action objects
//      actionObjects.foreach(println)