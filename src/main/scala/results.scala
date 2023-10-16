package com.lsc

import com.fasterxml.jackson.databind.ObjectMapper
import com.fasterxml.jackson.dataformat.yaml.YAMLFactory
import org.yaml.snakeyaml.Yaml

import java.io.File
import java.nio.file.{Files, Paths}
import scala.jdk.CollectionConverters.*


object CombinedParser {
  def main(args: Array[String]): Unit = {
    def parseEdge(edge: String): (Int, Int) = {
      val values = edge.trim.split("-")
      (values(0).toInt, values(1).toInt)
    }
    // Parsing data from the results file
    try {
      val data = new String(Files.readAllBytes(Paths.get(args(0)))).trim
      val categories = data.split("\n")

      val parsedData = categories.foldLeft(
        (
          List.empty[Int], // nodesModified
          List.empty[Int], // nodesRemoved
          List.empty[Int], // nodesAdded
          Map.empty[Int, Int], // edgesRemoved
          Map.empty[Int, Int], // edgesAdded
          Map.empty[Int, Int], // edgesModified
          Set.empty[(Int, Int)], // edgeUnchaged
          Set.empty[Int] // nodesUnchanged
        )
      ) { (acc, category) =>
        val parts = category.trim.split(" : ")
        val categoryType = parts(0).trim
        val items = parts(1).trim.split(";")

        categoryType match {
          case "edgesRemoved" => (acc._1, acc._2, acc._3, items.map(parseEdge).toMap, acc._5, acc._6, acc._7, acc._8)
          case "newEdgesAdded" => (acc._1, acc._2, acc._3, acc._4, items.map(parseEdge).toMap, acc._6, acc._7, acc._8)
          case "edgesModified" => (acc._1, acc._2, acc._3, acc._4, acc._5, items.map(parseEdge).toMap, acc._7, acc._8)
          case "edgesUnchanged" => (acc._1, acc._2, acc._3, acc._4, acc._5, acc._6, items.map(parseEdge).toSet, acc._8)
          case "newNodesAdded" => (acc._1, acc._2, items.map(_.toInt).toList, acc._4, acc._5, acc._6, acc._7, acc._8)
          case "nodesModified" => (items.map(_.toInt).toList, acc._2, acc._3, acc._4, acc._5, acc._6, acc._7, acc._8)
          case "nodesRemoved" => (acc._1, items.map(_.toInt).toList, acc._3, acc._4, acc._5, acc._6, acc._7, acc._8)
          case "nodesUnchanged" => (acc._1, acc._2, acc._3, acc._4, acc._5, acc._6, acc._7, items.map(_.toInt).toSet)
          case _ =>
            println(s"Unknown category: $categoryType")
            acc
        }
      }

      val (nodesModified, nodesRemoved, nodesAdded, edgesRemoved, edgesAdded, edgesModified, edgeUnchaged, nodesUnchanged) = parsedData

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

      // ... [keep the rest of your code above here]

      val initialResults = (0, 0, 0) // (atl, ctl, wtl)

      val results = Seq(
        (edgesRemoved, removedEdgesYaml),
        (edgesAdded, addedEdgesYaml),
        (edgesModified, modifiedEdgesYaml)
      ).foldLeft(initialResults) { case ((atlAcc, ctlAcc, wtlAcc), (parsedEdges, yamlEdges)) =>
        val atlUpdate = parsedEdges.count { case (src, tgt) => yamlEdges.get(src).contains(tgt) }
        val wtlUpdate = parsedEdges.count { case (src, tgt) => !yamlEdges.get(src).contains(tgt) }
        val ctlUpdate = yamlEdges.count { case (src, tgt) => !parsedEdges.get(src).contains(tgt) }

        (atlAcc + atlUpdate, ctlAcc + ctlUpdate, wtlAcc + wtlUpdate)
      }

      val (atl, ctl, wtl) = (
        results._1 +
          nodesRemoved.count(removedNodesYaml.contains) +
          nodesAdded.count(addedNodesYaml.values.toList.contains) +
          nodesModified.count(modifiedNodesYaml.contains) +
          (edgeUnchaged.size - edgeUnchaged.intersect((addedEdgesYaml ++ removedEdgesYaml ++ modifiedEdgesYaml).toSet).size) +
          (nodesUnchanged.size - nodesUnchanged.intersect((addedNodesYaml.values.toList ++ removedNodesYaml ++ modifiedNodesYaml).toSet).size),
        results._2,
        results._3
      )


      //      var atl, ctl, wtl = 0
//      edgesRemoved.foreach {
//        case (source, target) =>
//          if (removedEdgesYaml.contains(source) && removedEdgesYaml(source) == target) {
//            atl += 1
//          } else  {
//            wtl += 1
//          }
//      }
//      removedEdgesYaml.foreach{
//        case (source, target) =>
//          if (!(edgesRemoved.contains(source) && edgesRemoved(source) == target)) {
//            ctl += 1
//          }
//      }
//
//      edgesAdded.foreach {
//        case (source, target) =>
//          if (addedEdgesYaml.contains(source) && addedEdgesYaml(source) == target) {
//            atl += 1
//          } else  {
//            wtl += 1
//          }
//      }
//      addedEdgesYaml.foreach {
//        case (source, target) =>
//          if (!(edgesAdded.contains(source) && edgesAdded(source) == target)) {
//            ctl += 1
//          }
//      }
//
//
//      edgesModified.foreach {
//        case (source, target) =>
//          if (modifiedEdgesYaml.contains(source) && modifiedEdgesYaml(source) == target) {
//            atl += 1
//            print("edgeAdded")
//            println(atl)
//          } else  {
//            wtl += 1
//          }
//      }
//      modifiedEdgesYaml.foreach {
//        case (source, target) =>
//          if (!(edgesModified.contains(source) && edgesModified(source) == target)) {
//            ctl += 1
//          }
//      }
//
//      // Comparison for nodesRemoved
//      nodesRemoved.foreach { node =>
//        if (removedNodesYaml.contains(node)) {
//          atl += 1
//        } else {
//          wtl += 1
//        }
//      }
//      removedNodesYaml.foreach { node =>
//        if (!(nodesRemoved.contains(node))) {
//          ctl += 1
//        }
//      }
//
//
//      // Comparison for nodesAdded
//      val addedNodesYamlList = addedNodesYaml.values.toList
//      nodesAdded.foreach { node =>
//        if (addedNodesYamlList.contains(node)) {
//          atl += 1
//          //          print("noderemoved")
//          //          println(atl)
//        } else {
//          wtl += 1
//        }
//      }
//      addedNodesYamlList.foreach { node =>
//        if (!nodesAdded.contains(node)) {
//          ctl += 1
//        }
//      }
//
//
//      // Comparison for nodesModified
//      nodesModified.foreach { node =>
//        if (modifiedNodesYaml.contains(node)) {
//          atl += 1
//        } else  {
//          wtl += 1
//        }
//      }
//      modifiedNodesYaml.foreach { node =>
//        if (!(nodesModified.contains(node))) {
//          ctl += 1
//        }
//      }
//
//      val combinedEdge = (addedEdgesYaml ++ removedEdgesYaml ++ modifiedEdgesYaml).toSet
//      val unchangedEdgeCalc = edgeUnchaged.intersect(combinedEdge)
//      if(unchangedEdgeCalc.size == 0)
//      {
//        atl += edgeUnchaged.size
//      }
//      else
//        atl += edgeUnchaged.size - unchangedEdgeCalc.size
//
//      val addedNodesSet: List[Int] = addedNodesYaml.values.toList
//      val combinedNodes = (addedNodesSet ++ removedNodesYaml ++ modifiedNodesYaml).toSet
//      val unchangedNodeCalc = nodesUnchanged.intersect(combinedNodes)
//      if (unchangedNodeCalc.size == 0) {
//        atl += nodesUnchanged.size
//      }
//      else
//        atl += nodesUnchanged.size - unchangedNodeCalc.size
//
//      println(s"ATL: $atl")
//      println(s"CTL: $ctl")
//      println(s"WTL: $wtl")

      val dtl  = 0
      val GTL: Float = atl.toFloat + dtl.toFloat
      val BTL: Float = ctl.toFloat + wtl.toFloat
      val RTL: Float = GTL + BTL
      println(RTL)
      println(GTL)
      println(BTL)
      val VPR: Double = ((GTL - BTL)/(2*RTL)) + 0.5
      println("VPR: "+ VPR)
      val ACC: Double = GTL/RTL
      println("Accuracy: "+ACC)
      val BTLR: Double = wtl / RTL
      println("BTLR: "+ BTLR)
    } catch {
      case e: Exception =>
        println("Error processing the files: " + e.getMessage)
    }
  }
}