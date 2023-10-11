package com.lsc

import com.lsc.MyMain.logger

import scala.collection.mutable.ListBuffer
import scala.io.Source
import scala.util.Try
import org.apache.hadoop.fs.Path
import org.apache.hadoop.conf.*
import org.apache.hadoop.io.*
import org.apache.hadoop.util.*
import org.apache.hadoop.mapred.*

import java.io.InputStream
import java.net.URL
import java.io.IOException
import java.util
import scala.jdk.CollectionConverters.*
import scala.math.{pow, sqrt}

object NodeSimilarityScore:
  class Map extends MapReduceBase with Mapper[LongWritable, Text, Text, Text]:

    var attributeRanges: scala.collection.immutable.Map[String, (Double, Double)] = _

    override def configure(jobConf: JobConf): Unit = {
      val filePath = "https://graphsimilaritycs441.s3.amazonaws.com/inputs/AttributeRanges.txt"
      val source: scala.io.BufferedSource = if (filePath.startsWith("http://") || filePath.startsWith("https://")) {
        val url = new URL(filePath)
        scala.io.Source.fromInputStream(url.openStream())
      } else {
        val newFilePath = "/Users/sambhavjain/IdeaProjects/MapReduceSampleProject/src/main/resources/input/AttributeRanges.txt"
        scala.io.Source.fromFile(newFilePath)
      }
      try {
        logger.info(s"Inside Configure")
        attributeRanges = source.getLines().map { line =>
          val parts = line.split(": ")
          val range = parts(1).split(" to ").map(_.toDouble)
          parts(0) -> (range(0), range(1))
        }.toMap

        // Print the populated attributeRanges to console (for debugging)
        attributeRanges.foreach(println)

        // Close the buffered source
        source.close()

      } catch {
        case e: Exception =>
          // Print error details to console
          println(s"Error reading the file $filePath: ${e.getMessage}")
          e.printStackTrace()
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
        val nodes = line.split(" ") // Assuming the nodes are separated by a space
        logger.info(s"Inside Map")
        println(s"Nodes: ${nodes.mkString(", ")}")
        if (nodes.length == 2) {
          val originalAttributes = parseNodeAttributes(nodes(0))
          val perturbedAttributes = parseNodeAttributes(nodes(1))

          val similarityScore = calculateSimScore(originalAttributes, perturbedAttributes)
          // Rounding the similarity score
          val scale = 3 // Number of decimal places
          val roundedSimScore = BigDecimal(similarityScore).setScale(scale, BigDecimal.RoundingMode.HALF_UP).toDouble

          val originalId = originalAttributes("id").toString.toDouble.toInt
          val perturbedId = perturbedAttributes("id").toString.toDouble.toInt
          val outputKey = new Text(originalId.toString)
          val outputValue = new Text(s"${perturbedId},$roundedSimScore")
          println(s"Output Key: $outputKey, Output Value: $outputValue")
          output.collect(outputKey, outputValue)
        }
      } catch {
        case e: Exception =>
          println(s"Error in map function: ${e.getMessage}")
          e.printStackTrace()
      }
    }

    def calculateSimScore(originalAttributes: scala.collection.immutable.Map[String, Double], perturbedAttributes: scala.collection.immutable.Map[String, Double]): Double = {
      val attributeDiffs = ListBuffer[Double]()

      originalAttributes.keys.foreach { key =>
        if (key != "id") {
          // Exclude the "id" attribute
          val origAttr = originalAttributes(key)
          val perturbedAttr = perturbedAttributes(key)
          val diff = math.abs(origAttr - perturbedAttr)
          val range = attributeRanges.getOrElse(key, (0.0, 1.0)) // Default range if attribute is not found

          println(s"Original $key: $origAttr")
          println(s"Perturbed $key: $perturbedAttr")
          println(s"Diff: $diff, Range: $range")

          // Add the diff to the ListBuffer
          if (diff != 0.0) {
            attributeDiffs += (diff - range._1) / (range._2 - range._1)
          } else {
            attributeDiffs += diff
          }
        }
      }


      println(s"All attribute differences: ${attributeDiffs.toList}")
      // Calculate the Euclidean distance between normalized attribute differences
      val euclideanDistance = sqrt(attributeDiffs.map(diff => pow(diff, 2)).sum)
      println(s"Euclidean Distance: $euclideanDistance")
      // Normalize the distance to get the similarity score (higher values indicate more similarity)
      val similarityScore = 1.0 / (1.0 + euclideanDistance)
      similarityScore
    }

    def parseNodeAttributes(nodeStr: String): scala.collection.immutable.Map[String, Double] = {
      val attributeNames = Array("id", "children", "props", "currentDepth", "propValueRange",
        "maxDepth", "maxBranchingFactor", "maxProperties", "storedValue")

      val attributeValues = nodeStr.substring(nodeStr.indexOf('(') + 1, nodeStr.indexOf(')')).split(",").map(_.toDouble)

      // Check to make sure there is an attribute name for every attribute value, otherwise throw an error
      if (attributeNames.length != attributeValues.length) {
        throw new IllegalArgumentException("Mismatch between attribute names and values.")
      }

      attributeNames.zip(attributeValues).toMap
    }


  class Reduce extends MapReduceBase with Reducer [Text, Text, Text, Text] {
    override def reduce(key: Text, values: java.util.Iterator[Text], output: OutputCollector[Text, Text], reporter: Reporter): Unit = {
      try {
        logger.info(s"Inside Reduce of Nodes")
        val valuesList = ListBuffer[(String, Double)]()
        values.asScala.foreach { value =>
          val parts = value.toString.trim.split(",")
          if (parts.length == 2) {
            val Array(nodeId, score) = parts
            valuesList += ((nodeId, score.toDouble))
          } else {
            println(s"Unexpected value format: ${value.toString}")
          }
        }

        val sortedList = valuesList.sortBy(-_._2)
        val concatenatedValues = sortedList.map { case (firstElement, score) =>
          s"$firstElement,$score"
        }.mkString(" ; ")

        println(s"Concatenated Values: $concatenatedValues")
        logger.info(s"Before output of Nodes")
        output.collect(new Text(key), new Text(concatenatedValues))
      }catch {
        case e: Exception =>
          logger.info(s"Exception Occured")
          System.err.println(s"Error in reduce function: ${e.getMessage}")
          e.printStackTrace()
      }
    }
  }

  def runMapReduceNodes(inputPath: String, outputPath: String): Boolean =
    logger.info(s"NodeSimilarityScore Started")
    val conf: JobConf = new JobConf(this.getClass)
    conf.setJobName("NodeSimilarityScore")
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
    logger.info(s"Output Path: $outputPath")
    FileOutputFormat.setOutputPath(conf, new Path(outputPath))
    logger.info(s"After Output Path: $outputPath")
    val jobStatus = JobClient.runJob(conf)
    logger.info(s"Job completed with status: ${jobStatus.isSuccessful}")
    jobStatus.isSuccessful