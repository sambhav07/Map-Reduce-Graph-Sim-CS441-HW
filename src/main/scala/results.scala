package com.lsc

import com.fasterxml.jackson.databind.ObjectMapper
import com.fasterxml.jackson.dataformat.yaml.YAMLFactory
import org.yaml.snakeyaml.Yaml

import java.io.File
import java.nio.file.{Files, Paths}
import scala.jdk.CollectionConverters.*


object CombinedParser {
  def main(args: Array[String]): Unit = {

    // Parsing data from the results file
    try {
      val data = new String(Files.readAllBytes(Paths.get(args(0)))).trim
      val categories = data.split("\n")

      var nodesModified, nodesRemoved = List.empty[Int]
      var edgesRemoved, edgesAdded, edgesModified, nodesAdded = Map.empty[Int, Int]
      var edgeUnchaged = Set.empty[(Int, Int)]
      var nodesUnchanged = Set.empty[Int]

      categories.foreach { category =>
        val parts = category.trim.split(" : ")
        val categoryType = parts(0).trim
        val items = parts(1).trim.split(";")

        categoryType match {
          case "edgesRemoved" =>
            edgesRemoved = items.map { edge =>
              val values = edge.trim.split("-")
              (values(0).toInt, values(1).toInt)
            }.toMap
          case "edgesAdded" =>
            edgesAdded = items.map { edge =>
              val values = edge.trim.split("-")
              (values(0).toInt, values(1).toInt)
            }.toMap
          case "edgesModified" =>
            edgesModified = items.map { edge =>
              val values = edge.trim.split("-")
              (values(0).toInt, values(1).toInt)
            }.toMap
          case "edgesUnchanged" =>
            edgeUnchaged = items.map { edge =>
              val values = edge.trim.split("-")
              (values(0).toInt, values(1).toInt)
            }.toSet
          case "nodesAdded" =>
            nodesAdded = items.map { edge =>
              val values = edge.trim.split("-")
              (values(0).toInt, values(1).toInt)
            }.toMap
          case "nodesModified" =>
            nodesModified = items.map(_.toInt).toList
          case "nodesRemoved" =>
            nodesRemoved = items.map(_.toInt).toList
          case "nodesUnchanged" =>
            nodesUnchanged = items.map(_.toInt).toSet
          case _ =>
            println(s"Unknown category: $categoryType")
        }
      }

      // Parsing data from the second YAML file
      val yamlData = new String(Files.readAllBytes(Paths.get(args(1)))).replaceAll("\t", "  ")
      val yaml = new Yaml()
      val yamlParsedData = yaml.load(yamlData).asInstanceOf[java.util.Map[String, Any]]

      val nodes = yamlParsedData.get("Nodes").asInstanceOf[java.util.Map[String, Any]]
      val edges = yamlParsedData.get("Edges").asInstanceOf[java.util.Map[String, Any]]

      val modifiedNodesYaml: List[Int] = Option(nodes.get("Modified")).map(_.asInstanceOf[java.util.List[Int]].asScala.toList).getOrElse(List.empty)
      val removedNodesYaml: List[Int] = Option(nodes.get("Removed")).map(_.asInstanceOf[java.util.List[Int]].asScala.toList).getOrElse(List.empty)
      val addedNodesYaml: Map[Int, Int] = Option(nodes.get("Added")).map(_.asInstanceOf[java.util.Map[Int, Int]].asScala.toMap).getOrElse(Map.empty)
      val modifiedEdgesYaml: Map[Int, Int] = Option(edges.get("Modified")).map(_.asInstanceOf[java.util.Map[Int, Int]].asScala.toMap).getOrElse(Map.empty)
      val addedEdgesYaml: Map[Int, Int] = Option(edges.get("Added")).map(_.asInstanceOf[java.util.Map[Int, Int]].asScala.toMap).getOrElse(Map.empty)
      val removedEdgesYaml: Map[Int, Int] = Option(edges.get("Removed")).map(_.asInstanceOf[java.util.Map[Int, Int]].asScala.toMap).getOrElse(Map.empty)

      var atl, ctl, wtl = 0
      edgesRemoved.foreach {
        case (source, target) =>
          if (removedEdgesYaml.contains(source) && removedEdgesYaml(source) == target) {
            atl += 1
          } else  {
            wtl += 1
          }
      }
      removedEdgesYaml.foreach{
        case (source, target) =>
          if (!(edgesRemoved.contains(source) && edgesRemoved(source) == target)) {
            ctl += 1
          }
      }

      edgesAdded.foreach {
        case (source, target) =>
          if (addedEdgesYaml.contains(source) && addedEdgesYaml(source) == target) {
            atl += 1
          } else  {
            wtl += 1
          }
      }
      addedEdgesYaml.foreach {
        case (source, target) =>
          if (!(edgesAdded.contains(source) && edgesAdded(source) == target)) {
            ctl += 1
          }
      }


      edgesModified.foreach {
        case (source, target) =>
          if (modifiedEdgesYaml.contains(source) && modifiedEdgesYaml(source) == target) {
            atl += 1
            print("edgeAdded")
            println(atl)
          } else  {
            wtl += 1
          }
      }
      modifiedEdgesYaml.foreach {
        case (source, target) =>
          if (!(edgesModified.contains(source) && edgesModified(source) == target)) {
            ctl += 1
          }
      }

      // Comparison for nodesRemoved
      nodesRemoved.foreach { node =>
        if (removedNodesYaml.contains(node)) {
          atl += 1
        } else {
          wtl += 1
        }
      }
      removedNodesYaml.foreach { node =>
        if (!(nodesRemoved.contains(node))) {
          ctl += 1
        }
      }


      // Comparison for nodesAdded
      nodesAdded.foreach {
        case (node, value) =>
          if (addedNodesYaml.contains(node) && addedNodesYaml(node) == value) {
            atl += 1
          } else  {
            wtl += 1
          }
      }
      addedNodesYaml.foreach {
        case (node, value) =>
          if (!(nodesAdded.contains(node) && nodesAdded(node) == value)) {
            ctl += 1
          }
      }


      // Comparison for nodesModified
      nodesModified.foreach { node =>
        if (modifiedNodesYaml.contains(node)) {
          atl += 1
        } else  {
          wtl += 1
        }
      }
      modifiedNodesYaml.foreach { node =>
        if (!(nodesModified.contains(node))) {
          ctl += 1
        }
      }

      val combinedEdge = (addedEdgesYaml ++ removedEdgesYaml ++ modifiedEdgesYaml).toSet
      val unchangedEdgeCalc = edgeUnchaged.intersect(combinedEdge)
      if(unchangedEdgeCalc.size == 0)
      {
        atl += edgeUnchaged.size
      }
      else
        atl += edgeUnchaged.size - unchangedEdgeCalc.size

      val addedNodesSet: List[Int] = addedNodesYaml.values.toList
      val combinedNodes = (addedNodesSet ++ removedNodesYaml ++ modifiedNodesYaml).toSet
      val unchangedNodeCalc = nodesUnchanged.intersect(combinedNodes)
      if (unchangedNodeCalc.size == 0) {
        atl += nodesUnchanged.size
      }
      else
        atl += nodesUnchanged.size - unchangedNodeCalc.size

      println(s"ATL: $atl")
      println(s"CTL: $ctl")
      println(s"WTL: $wtl")

      val dtl  = 0
      val GTL: Float = atl.toFloat + dtl.toFloat
      val BTL: Float = ctl.toFloat + wtl.toFloat
      val RTL: Float = GTL + BTL
      println(RTL)
      println(GTL)
      println(BTL)
      val VPR: Double = ((GTL - BTL)/(2*RTL)) + 0.5
      println(VPR)
      val ACC: Double = GTL/RTL
      println(ACC)
    } catch {
      case e: Exception =>
        println("Error processing the files: " + e.getMessage)
    }
  }
}

