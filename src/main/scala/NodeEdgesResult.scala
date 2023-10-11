package com.lsc

import MyMain.logger

import org.apache.hadoop.fs.Path
import org.apache.hadoop.io.*
import org.apache.hadoop.mapred.*

import java.io.IOException
import java.util
import scala.collection.mutable
import scala.jdk.CollectionConverters.*

object NodeEdgesResult:
  class Map extends MapReduceBase with Mapper[LongWritable, Text, Text, Text]:

    private val modificationStatus = new Text()
    private val ID = new Text()

    @throws[IOException]
    override def map(
                      key: LongWritable,
                      value: Text,
                      output: OutputCollector[Text, Text],
                      reporter: Reporter): Unit = {
      logger.trace(s"Mapping value: $value") // Added trace log
      val values = value.toString.split(" : ")
      if (values(0).contains("-")) {
        output.collect(new Text("matchedEdges"), new Text(values(0).trim))
      } else {
        output.collect(new Text("matchedNodes"), new Text(values(0)))
      }
      val intValueOption: Option[Int] = values(0).toString.toIntOption

      intValueOption match {
        case Some(intValue) =>
          val nodevaluepair = values(1).split(" ; ")
          val getfirstvaluepair = nodevaluepair(0).split(",")
          if (getfirstvaluepair(1).toDouble == 1.0) {
            modificationStatus.set(new Text("nodesUnchanged"))
            ID.set(values(0))
          }
          else if (getfirstvaluepair(1).toDouble < 0.75) {
            modificationStatus.set(new Text("nodesRemoved"))
            ID.set(values(0))
            println(s"$modificationStatus,$ID")
          }
          else {
            modificationStatus.set(new Text("nodesModified"))
            ID.set(values(0))
            println(s"$modificationStatus,$ID")
          }

          for (i <- 1 until nodevaluepair.length) {
            val unmatched = nodevaluepair(i).split(",")(0)
            output.collect(new Text("unmatchedNodes"), new Text(unmatched))
          }
        case None =>
          logger.warn("The value does not convert to an integer, processing as edge data") // Added warn log
          val edgevaluepair = values(1).split(";")
          val getfirstvaluepair = edgevaluepair(0).split(",")
          if (getfirstvaluepair(1).toDouble == 1.0) {
            modificationStatus.set(new Text("edgesUnchanged"))
            ID.set(values(0))
          }
          else if (getfirstvaluepair(1).toDouble < 0.80) {
            modificationStatus.set(new Text("edgesRemoved"))
            ID.set(values(0))
            println(s"$modificationStatus,$ID")
          }
          else {
            modificationStatus.set(new Text("edgesModified"))
            ID.set(values(0))
            println(s"$modificationStatus,$ID")
          }

          for (i <- 1 until edgevaluepair.length) {
            val unmatched = edgevaluepair(i).split(",")(0).trim
            output.collect(new Text("unmatchedEdges"), new Text(unmatched))
          }
      }
      logger.info(s"Collected $modificationStatus with ID $ID") // Added info log
      output.collect(modificationStatus, ID)
    }



  class Reduce extends MapReduceBase with Reducer [Text, Text, Text, Text] {
    override def reduce(key: Text, values: java.util.Iterator[Text], output: OutputCollector[Text, Text], reporter: Reporter): Unit = {
      logger.info(s"Reducer called with key: $key") //

      val result = new StringBuilder()
      val itemsSet = new util.HashSet[String]()

      while (values.hasNext) {
        val item = values.next().toString
        itemsSet.add(item)
        result.append(item)
        if (values.hasNext) {
          result.append(";")
        }
      }

      NodeEdgesResult.Reduce.collections.synchronized {
        NodeEdgesResult.Reduce.collections.put(key.toString, itemsSet.asScala.toSet)
        logger.trace(s"Updated collections after processing $key: ${NodeEdgesResult.Reduce.collections}") // Changed from println to logger.trace
      }

      if (key.toString == "matchedNodes" || key.toString == "unmatchedNodes") {
        if (NodeEdgesResult.Reduce.collections.contains("matchedNodes") && NodeEdgesResult.Reduce.collections.contains("unmatchedNodes")) {
          val matchedNodes = NodeEdgesResult.Reduce.collections("matchedNodes")
          val unmatchedNodes = NodeEdgesResult.Reduce.collections("unmatchedNodes")
          val newNodesAdded = unmatchedNodes.diff(matchedNodes)

          if (newNodesAdded.nonEmpty) {
            val newNodesResult = new StringBuilder()
            newNodesAdded.foreach(node => {
              newNodesResult.append(node)
              newNodesResult.append(";")
            })
            newNodesResult.setLength(newNodesResult.length - 1) // Remove the last semicolon
            output.collect(new Text("newNodesAdded"), new Text(newNodesResult.toString()))
          }
        }
      } else if (key.toString == "matchedEdges" || key.toString == "unmatchedEdges") {
        if (NodeEdgesResult.Reduce.collections.contains("matchedEdges") && NodeEdgesResult.Reduce.collections.contains("unmatchedEdges")) {
          val matchedEdges = NodeEdgesResult.Reduce.collections("matchedEdges")
          val unmatchedEdges = NodeEdgesResult.Reduce.collections("unmatchedEdges")
          val newEdgesAdded = unmatchedEdges.diff(matchedEdges)

          println(s"Matched Edges: $matchedEdges")
          println(s"Unmatched Edges: $unmatchedEdges")
          println(s"New Edges Added: $newEdgesAdded")

          if (newEdgesAdded.nonEmpty) {
            val newEdgesResult = new StringBuilder()
            newEdgesAdded.foreach(edge => {
              newEdgesResult.append(edge)
              newEdgesResult.append(";")
            })
            newEdgesResult.setLength(newEdgesResult.length - 1) // Remove the last semicolon
            logger.info(s"New edges added: $newEdgesAdded")
            output.collect(new Text("newEdgesAdded"), new Text(newEdgesResult.toString()))
          }
        }
      } else if (result.nonEmpty) {
        output.collect(key, new Text(result.toString()))
      }

      logger.info(s"Reducer completed with key: $key")
    }}

  object Reduce {
    val collections: mutable.Map[String, Set[String]] = mutable.Map()
  }

  def runMapReduceResult(inputPath1: String, inputPath2: String, outputPath: String) =

    val conf: JobConf = new JobConf(this.getClass)
    conf.setJobName("NodeEdgesResult")
    conf.set("mapreduce.job.maps", "1")
    conf.set("mapreduce.job.reduces", "1")
    conf.setOutputKeyClass(classOf[Text])
    conf.setOutputValueClass(classOf[Text])
    conf.setMapperClass(classOf[Map])
    conf.setReducerClass(classOf[Reduce])
    conf.setInputFormat(classOf[TextInputFormat])
    conf.setOutputFormat(classOf[TextOutputFormat[Text, Text]])
    conf.set("mapreduce.output.textoutputformat.separator", " : ")
    val inputPaths = Array(inputPath1, inputPath2)
    FileInputFormat.setInputPaths(conf, inputPaths.map(new Path(_)): _*)
    FileOutputFormat.setOutputPath(conf, new Path(outputPath))
    val jobStatus = JobClient.runJob(conf)
    jobStatus.isSuccessful