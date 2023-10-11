package com.lsc

import com.lsc.MyMain.logger
import org.apache.hadoop.conf.*
import org.apache.hadoop.fs.Path
import org.apache.hadoop.io.*
import org.apache.hadoop.mapred.*
import org.apache.hadoop.util.*

import java.io.IOException
import java.util
import scala.collection.mutable.ListBuffer
import scala.io.Source
import scala.jdk.CollectionConverters.*
import scala.math.{pow, sqrt}
import scala.util.Try
import org.apache.hadoop.fs.{FileSystem, Path}

import scala.io.BufferedSource
object EdgeSimilarityScore:
  class Map extends MapReduceBase with Mapper[LongWritable, Text, Text, Text]:

    var attributeRanges: scala.collection.immutable.Map[String, (Double, Double)] = _

    override def configure(jobConf: JobConf): Unit = {
      val filePath = "https://graphsimilaritycs441.s3.amazonaws.com/inputs/ActionAttributeRanges.txt"
      val source: scala.io.BufferedSource = if (filePath.startsWith("http://") || filePath.startsWith("https://")) {
        // For S3 path
        val uri = new java.net.URI(filePath)
        val fs = FileSystem.get(uri, jobConf)
        val stream = fs.open(new Path(uri))
        scala.io.Source.fromInputStream(stream)
      } else {
        // For local file path
        val newFilePath = "/Users/sambhavjain/IdeaProjects/MapReduceSampleProject/src/main/resources/input/ActionAttributeRanges.txt"
        scala.io.Source.fromFile(filePath)
      }
      try {
        // Process the buffered source
        attributeRanges = source.getLines().map { line =>
          val parts = line.split(": ")
          val range = parts(1).split(" to ").map(_.toDouble)
          parts(0) -> (range(0), range(1))
        }.toMap

        // Print the populated attributeRanges to console (for debugging)
        attributeRanges.foreach(println)

        } catch {
        case e: Exception =>
          // Print error details to console
          logger.error(s"Error reading the file $filePath: ${e.getMessage}") // Changed to logger.error
          e.printStackTrace()
      } finally {
        source.close()
      }
    }

    @throws[IOException]
    override def map(
                      key: LongWritable,
                      value: Text,
                      output: OutputCollector[Text, Text],
                      reporter: Reporter): Unit = {
      try {
        val line = value.toString
        val actions = line.split(" ") // Assuming the nodes are separated by a space
        actions.foreach(println)

        if (actions.length == 2) {
          val (originalNodeIds, originalAttributes) = parseActionAttributes(actions(0))
          val (perturbedNodeIds, perturbedAttributes) = parseActionAttributes(actions(1))

          val similarityScore = calculateSimScore(originalAttributes, perturbedAttributes)

          val scale = 3  // Number of decimal places
          val roundedSimScore = BigDecimal(similarityScore).setScale(scale, BigDecimal.RoundingMode.HALF_UP).toDouble

          println(s"Original Node IDs: $originalNodeIds")
          println(s"Perturbed Node IDs: $perturbedNodeIds")
          // Prepare the key as "originalNodeId-perturbedNodeId"
          val outputKey = new Text(s"${originalNodeIds._1}-${originalNodeIds._2}")

          // Prepare the value as "perturbedActionNode1Id-perturbedActionNode2Id,similarityScore"
          val outputValue = new Text(s"${perturbedNodeIds._1}-${perturbedNodeIds._2},$roundedSimScore")

          // Print for debugging purposes (optional)
          println(s"outputKey: $outputKey, outputValue: $outputValue")

          // Collect the key-value pair for the MapReduce job
          output.collect(outputKey, outputValue)
        }
      } catch {
        case e: Exception =>
          println(s"Error in map function: ${e.getMessage}")
          e.printStackTrace()
      }
    }

    def calculateSimScore(originalAttributes: Array[(String, Double)], perturbedAttributes: Array[(String, Double)]): Double = {
      // Ensure both attribute arrays have the same length
      if (originalAttributes.length != perturbedAttributes.length) {
        throw new IllegalArgumentException("The attribute arrays must have the same length")
      }

      originalAttributes.zip(perturbedAttributes).foreach { case ((originalName, originalValue), (perturbedName, perturbedValue)) =>
        println(s"Original Attribute $originalName: $originalValue")
        println(s"Perturbed Attribute $perturbedName: $perturbedValue")
      }

      // Calculate attribute differences and normalize based on the attribute ranges
      val attributeDiffs = originalAttributes.zip(perturbedAttributes).map {
        case ((originalName, originalValue), (perturbedName, perturbedValue)) =>
          val diff = math.abs(originalValue - perturbedValue)
          val range = attributeRanges.getOrElse(originalName, (0.0, 1.0))  // Now using attribute name to get the range

          println(s"Diff: $diff, Range: $range")

          if (diff > 0)
            (diff - range._1) / (range._2 - range._1)
          else
            0
      }

      // Calculate the Euclidean distance between normalized attribute differences
      val euclideanDistance = math.sqrt(attributeDiffs.map(diff => math.pow(diff, 2)).sum)

      // Normalize the distance to get the similarity score (higher values indicate more similarity)
      val similarityScore = 1.0 / (1.0 + euclideanDistance)

      similarityScore
    }

    // Function to parse NodeObject attributes from a string
    def parseActionAttributes(actionStr: String): ((Int, Int), Array[(String, Double)]) = {
      val pattern = """Action\((\d+),NodeObject\(([^)]+)\),NodeObject\(([^)]+)\),([^,]+),([^,]+),([^,]+),([^)]+)\)""".r
      actionStr match {
        case pattern(actionType, node1Str, node2Str, fromId, toId, resultingValueStr, cost) =>
          val node1Id = node1Str.split(",")(0).toInt
          val node2Id = node2Str.split(",")(0).toInt

          val resultingValue = resultingValueStr match {
            case s if s.startsWith("Some") => s.stripPrefix("Some(").stripSuffix(")").toDouble
            case _ => -1.0 // Handling the None case
          }

          val actionAttributeNames = Array("actionType", "fromId", "toId", "resultingValue", "cost") // Replace with your actual attribute names

          val actionAttributes = actionAttributeNames.zip(Array(actionType.toDouble, fromId.toDouble, toId.toDouble, resultingValue, cost.toDouble))

          println(s"Node1 ID: $node1Id")
          println(s"Node2 ID: $node2Id")

          ((node1Id, node2Id), actionAttributes)

        case _ =>
          println(s"Invalid action string format: $actionStr")
          ((-1, -1), Array.empty[(String, Double)]) // or handle the error as appropriate
      }
    }


  class Reduce extends MapReduceBase with Reducer [Text, Text, Text, Text] {
    override def reduce(key: Text, values: java.util.Iterator[Text], output: OutputCollector[Text, Text], reporter: Reporter): Unit = {
      try {
        val valuesList = ListBuffer[(String, Double)]()
        values.asScala.foreach { value =>
          val parts = value.toString.trim.split(",")
          if (parts.length == 2) {
            val Array(actionPair, score) = parts
            valuesList += ((actionPair, score.toDouble))
          } else {
            println(s"Unexpected value format: ${value.toString}")
          }
        }
        val sortedList = valuesList.sortBy(-_._2) // Sort by similarity score in descending order
        val concatenatedValues = sortedList.map {
          case (actionPair, score) => s"$actionPair,$score"
        }.mkString(" ; ")

        println(s"Concatenated Values: $concatenatedValues")
        output.collect(new Text(key), new Text(concatenatedValues))
      } catch {
        case e: Exception =>
          System.err.println(s"Error in reduce function: ${e.getMessage}")
          e.printStackTrace()
      }
    }
  }

  def runMapReduceEdges(inputPath: String, outputPath: String): Boolean =
    logger.info(s"EdgeSimilarityScore Started")
    val conf: JobConf = new JobConf(this.getClass)
    conf.setJobName("EdgeSimilarityScore")
    conf.set("mapreduce.job.maps", "1")
    conf.set("mapreduce.job.reduces", "1")
    conf.setOutputKeyClass(classOf[Text])
    conf.setOutputValueClass(classOf[Text])
    conf.setMapperClass(classOf[Map])
    conf.setReducerClass(classOf[Reduce])
    conf.setInputFormat(classOf[TextInputFormat])
    conf.setOutputFormat(classOf[TextOutputFormat[Text, Text]])
    conf.set("mapreduce.output.textoutputformat.separator", " : ")
    FileInputFormat.setInputPaths(conf, new Path(inputPath))
    FileOutputFormat.setOutputPath(conf, new Path(outputPath))
    val jobStatus = JobClient.runJob(conf)
    jobStatus.isSuccessful