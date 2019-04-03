package iot
import org.apache.spark.{SparkConf, SparkContext}
object iot_lab2 {
  def findGroupsTailRecursive(list: List[(Long, String)], frequencyMap: scala.collection.Map[String, Int], threshold: Int) = {
    def findGroups(groupMapping: List[(Int, Long)], list: List[(Long, String)], frequencyMap: scala.collection.Map[String, Int], threshold: Int, acc: Long = 0, group: Int = 0): List[(Int, Long)] = list match {
      case Nil => groupMapping
      case h::t => {
        if(acc + frequencyMap(h._2) < threshold) findGroups(Tuple2(group, h._1)::groupMapping , t, frequencyMap, threshold,acc + frequencyMap(h._2), group)
        else if(acc == 0 && frequencyMap(h._2) >= threshold) findGroups(Tuple2(-1, h._1)::groupMapping, t, frequencyMap, threshold, acc, group)
        else findGroups(groupMapping, list,  frequencyMap, threshold, 0, group + 1)
      }
    }
    findGroups(Nil, list, frequencyMap, threshold)
  }

  def main(args: Array[String]): Unit = {
    val conf = new SparkConf().setMaster(args(0)).setAppName("lab2")
    val sc = new SparkContext(conf)
    val threshold = 2000
    val text = sc.textFile(args(1))
    val descendingWordFrequency = text.flatMap(line => line.split(" ")).map(word => (word, 1)).reduceByKey(_ + _).sortBy(_._2, ascending = false)
    val relabels = descendingWordFrequency.zipWithIndex().map{ case (word, index) => (word._1, index+1)}.collectAsMap()
    val relabeledMatrix = text.map(file => file.split("\n").map(each_line => each_line.split(" ").map(word => relabels(word)).sorted.mkString(" ")).mkString("\n")).collect()
    val sortedRelabeledMatrix = relabeledMatrix.map(line => (line.split(" ").length,line)).sortWith(_._1 > _._1).map(_._2)
    val groups = findGroupsTailRecursive(relabels.map(_.swap).toList.sortWith(_._1 > _._1), descendingWordFrequency.collectAsMap(), threshold).sortWith(_._1 < _._1).map(_.swap).toMap
    /***  Older Line
      *   Instead of groupBy filtering is done while extracting groupId's for each line
      *   groupId's are saved in Set and after that collected and tuples of groupId and line are made
      *  val groupedMatrix = sortedRelabeledMatrix.flatMap(line => line.split(" ").map(relabeledWord => (groups(relabeledWord.toInt), line))).groupBy(_._1).map{case (k,v) => (k, v.groupBy(_._2).map{case (k,v) => k})}.foreach(groupTuple => sc.parallelize(Seq(groupTuple._2.mkString("\n"))).saveAsTextFile("group_" + groupTuple._1 + "_output"))
      */
    val groupedMatrix = sortedRelabeledMatrix.flatMap(line => line.split(" ").map(relabeledWord => groups(relabeledWord.toInt)).toSet.map((grpId: Int) => (grpId, line))).groupBy(_._1).map{case (k,v) => (k, v.groupBy(_._2).map{case (k,v) => k})}.foreach(groupTuple => sc.parallelize(Seq(groupTuple._2.mkString("\n"))).saveAsTextFile("group_" + groupTuple._1 + "_output"))
  }
}