package com.lsc
import scala.collection.mutable
import org.apache.hadoop.io._
import org.apache.hadoop.mapred._
import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should.Matchers

class NodeSimilarityScoreTest extends AnyFlatSpec with Matchers {

  class MockIterator[T](items: List[T]) extends java.util.Iterator[T] {
    private val internalIterator = items.iterator

    override def hasNext: Boolean = internalIterator.hasNext


    override def next(): T = {
      if (!hasNext) {
        throw new NoSuchElementException
      }
      internalIterator.next()
    }
  }

  class TestableMap extends NodeSimilarityScore.Map {
    attributeRanges = Map(
      "maxDepth" -> (0.0, 4.0),
      "currentDepth" -> (1.0, 1.0),
      "children" -> (0.0, 6.0),
      "maxBranchingFactor" -> (0.0, 60.0),
      "storedValue" -> (0.0011174283915980077, 0.9986163180697054),
      "maxProperties" -> (0.0, 19.0),
      "propValueRange" -> (0.0, 99.0),
      "props" -> (0.0, 19.0)
    )
  }

  class MockOutputCollector extends OutputCollector[Text, Text] {
    val collectedData: mutable.Map[Text, Seq[Text]] = mutable.Map()

    override def collect(key: Text, value: Text): Unit = {
      val currentData = collectedData.getOrElse(key, Seq())
      collectedData(key) = currentData :+ value
    }
  }

  behavior of "NodeSimilarityScore.Map"

  it should "correctly process node pairs" in {
    val mapper = new TestableMap()
    val output = new MockOutputCollector()
    val reporter = null // as we aren't using reporter in the map method

    val input = new Text("NodeObject(101,6,2,1,54,1,1,14,0.9699899997361618) NodeObject(25,5,16,1,32,2,5,10,0.9807993565469415)")
    mapper.map(null, input, output, reporter)

    // Validate output based on your logic
    output.collectedData should contain key new Text("101")
  }

  // tests for reducer.

  behavior of "NodeSimilarityScore.Reduce"

  it should "aggregate and sort similarity scores correctly" in {
    val reducer = new NodeSimilarityScore.Reduce()
    val output = new MockOutputCollector()

    val key = new Text("Node_101")
    val similarityScoresTexts = List(
      new Text("Node_102,0.96"),
      new Text("Node_103,0.95"),
      new Text("Node_104,0.97")
    )

    val values = new MockIterator[Text](similarityScoresTexts)
    reducer.reduce(key, values, output, null) // Using null for reporter as we aren't using it

    // Validate output based on your logic.
    // Assuming that the reducer sorts the nodes based on the similarity scores in descending order:
    val expectedOutput = "Node_104,0.97 ; Node_102,0.96 ; Node_103,0.95"
    output.collectedData(key) shouldBe Seq(new Text(expectedOutput))
  }
}
