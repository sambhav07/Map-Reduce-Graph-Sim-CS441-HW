package com.lsc

import org.apache.hadoop.io._
import org.apache.hadoop.mapred._
import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should.Matchers
import scala.collection.mutable

class EdgeSimilarityScoreTest extends AnyFlatSpec with Matchers {

  class TestableEdgeMap extends EdgeSimilarityScore.Map {
    attributeRanges = Map(
      "actionType" -> (0.0, 19.0),
      "fromId" -> (0.0, 221.0),
      "toId" -> (0.0, 196.0),
      "resultingValue" -> (0.0, 99.0),
      "cost" -> (4.438801375618029E-4, 0.9989410575329741)
    )
  }

  class MockOutputCollector extends OutputCollector[Text, Text] {
    val collectedData: mutable.Map[Text, mutable.Buffer[Text]] = mutable.Map()

    override def collect(key: Text, value: Text): Unit = {
      val currentData = collectedData.getOrElseUpdate(key, mutable.Buffer[Text]())
      currentData += value
    }
  }

  behavior of "EdgeSimilarityScore.Map"

  it should "correctly process action pairs" in {
    val mapper = new TestableEdgeMap()
    val output = new MockOutputCollector()
    val reporter = null // as we aren't using reporter in the map method

    val input = new Text("Action(5,NodeObject(245,4,18,1,43,2,4,7,0.8723269321055319),NodeObject(92,5,16,1,25,3,0,12,0.7323461857463088),170,25,Some(32),0.2649254536708584) Action(13,NodeObject(47,5,11,1,93,4,1,15,0.2809254075113349),NodeObject(200,6,0,1,55,3,5,10,0.6443832368706449),34,52,Some(84),0.5619467825373583)")
    mapper.map(null, input, output, reporter)

    // Validate output based on your logic. Example check:
    output.collectedData should contain key(new Text("245-92"))
    // Add more validations for the values
  }

  behavior of "EdgeSimilarityScore.Reduce"

  it should "aggregate and sort similarity scores correctly" in {
    val reducer = new EdgeSimilarityScore.Reduce()
    val output = new MockOutputCollector()

    val key = new Text("Action_170-25")
    val similarityScoresTexts = List(
      new Text("Action_171-26,0.96"),
      new Text("Action_172-27,0.95"),
      new Text("Action_173-28,0.97")
    )

    val values = new MockIterator[Text](similarityScoresTexts)
    reducer.reduce(key, values, output, null) // Using null for reporter as we aren't using it

    // Validate output based on your logic.
    // Assuming that the reducer sorts the actions based on the similarity scores in descending order:
    val expectedOutput = "Action_173-28,0.97 ; Action_171-26,0.96 ; Action_172-27,0.95"
    output.collectedData(key) shouldBe Seq(new Text(expectedOutput))
  }

  // MockIterator is an Iterator used for testing purposes
  class MockIterator[T](items: List[T]) extends java.util.Iterator[T] {
    private val iterator = items.iterator

    override def hasNext: Boolean = iterator.hasNext

    override def next(): T = iterator.next()
  }
}
