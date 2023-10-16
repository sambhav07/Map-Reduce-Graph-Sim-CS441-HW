package com.lsc

import org.apache.hadoop.io._
import org.apache.hadoop.mapred._
import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should.Matchers
import scala.jdk.CollectionConverters._
import scala.collection.mutable

class NodeEdgesResultTest extends AnyFlatSpec with Matchers {

  // Define the mock OutputCollector as an inner class
  class MockOutputCollector extends OutputCollector[Text, Text] {
    val collectedData: mutable.Map[Text, mutable.Buffer[Text]] = mutable.Map()

    override def collect(key: Text, value: Text): Unit = {
      val currentData = collectedData.getOrElseUpdate(key, mutable.Buffer[Text]())
      currentData += value
    }
  }

  behavior of "NodeEdgesResult.Map"

  it should "correctly process node values" in {
    val mapper = new NodeEdgesResult.Map()
    val output = new MockOutputCollector()
    val reporter = null // as we aren't using reporter in your map method

    val nodeInput = new Text("0 : 0,1.0 ; 310,0.785 ; 214,0.733")
    mapper.map(null, nodeInput, output, reporter)

    output.collectedData(new Text("matchedNodes")) should contain (new Text("0"))
    output.collectedData(new Text("unmatchedNodes")) should contain allOf (new Text("310"), new Text("214"))
  }

  it should "correctly process edge values" in {
    val mapper = new NodeEdgesResult.Map()
    val output = new MockOutputCollector()

    val edgeInput = new Text("0-18 : 0-18,1.0 ; 192-92,0.944 ; 271-255,0.936")
    mapper.map(null, edgeInput, output, null)

    output.collectedData(new Text("matchedEdges")) should contain (new Text("0-18"))
    output.collectedData(new Text("unmatchedEdges")) should contain allOf (new Text("192-92"), new Text("271-255"))
  }

  // Additional tests for the reducer can be written in a similar fashion by using the MockOutputCollector
  // that collects the data into a local data structure, and then asserting on that data.

  behavior of "NodeEdgesResult.Reduce"

  it should "combine node inputs correctly" in {
    val reducer = new NodeEdgesResult.Reduce()
    val output = new MockOutputCollector()

    // Example input for the reducer
    val matchedNodesKey = new Text("matchedNodes")
    val unmatchedNodesKey = new Text("unmatchedNodes")

    val matchedNodesValues = List(new Text("0"), new Text("1"))
    val unmatchedNodesValues = List(new Text("310"), new Text("214"), new Text("0"))

    // Call the reducer
    reducer.reduce(matchedNodesKey, matchedNodesValues.iterator.asJava, output, null)
    reducer.reduce(unmatchedNodesKey, unmatchedNodesValues.iterator.asJava, output, null)

    // Validate the output
    output.collectedData(new Text("newNodesAdded")).head.toString.split(";").map(_.trim) should contain allOf("310", "214")
  }

  it should "combine edge inputs correctly" in {
    val reducer = new NodeEdgesResult.Reduce()
    val output = new MockOutputCollector()

    // Example input for the reducer
    val matchedEdgesKey = new Text("matchedEdges")
    val unmatchedEdgesKey = new Text("unmatchedEdges")

    val matchedEdgesValues = List(new Text("0-18"))
    val unmatchedEdgesValues = List(new Text("192-92"), new Text("271-255"), new Text("0-18"))

    // Call the reducer
    reducer.reduce(matchedEdgesKey, matchedEdgesValues.iterator.asJava, output, null)
    reducer.reduce(unmatchedEdgesKey, unmatchedEdgesValues.iterator.asJava, output, null)

    // Validate the output
    output.collectedData(new Text("newEdgesAdded")).head.toString.split(";").map(_.trim) should contain allOf("192-92", "271-255")
  }

}
