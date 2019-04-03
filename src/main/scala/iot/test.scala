package iot

import java.util.Properties

import org.apache.kafka.clients.producer.{KafkaProducer, ProducerRecord}
import org.apache.kafka.common.serialization.StringDeserializer
import org.apache.spark._
import org.apache.spark.streaming._
import org.apache.spark.streaming.kafka010.ConsumerStrategies.Subscribe
import org.apache.spark.streaming.kafka010.LocationStrategies.PreferConsistent
import org.apache.spark.streaming.kafka010._
import org.json4s.jackson.JsonMethods._

object test {
  def main(args: Array[String]): Unit = {
    val conf = new SparkConf().setMaster("local[3]").setAppName("NetworkWordCount")
    val ssc = new StreamingContext(conf, Seconds(1))

    val kafkaParams = Map[String, Object](
      "bootstrap.servers" -> "localhost:9092",
      "key.deserializer" -> classOf[StringDeserializer],
      "value.deserializer" -> classOf[StringDeserializer],
      "group.id" -> "use_a_separate_group_id_for_each_stream",
      "auto.offset.reset" -> "earliest",
      "enable.auto.commit" -> (false: java.lang.Boolean)
    )

    val topics = Array("output10")
    val topics1 = Array("output11")
    val topics2 = Array("output13")
    val topics3 = Array("output14")
    val kafkaSink = "output5"

    val stream = KafkaUtils.createDirectStream[String, String](ssc, PreferConsistent, Subscribe[String, String](topics, kafkaParams))
    val stream1 = KafkaUtils.createDirectStream[String, String](ssc, PreferConsistent, Subscribe[String, String](topics1, kafkaParams))
    val stream2 = KafkaUtils.createDirectStream[String, String](ssc, PreferConsistent, Subscribe[String, String](topics2, kafkaParams))
    val stream3 = KafkaUtils.createDirectStream[String, String](ssc, PreferConsistent, Subscribe[String, String](topics3, kafkaParams))

    val ds1 = stream.map(record => parse(record.value).values.asInstanceOf[Map[String, Any]]).map(record => (record("creationTime"), record("pollId"), record("kpiValues"))).map(recordTuple => (recordTuple._1.asInstanceOf[Map[String, Long]]("$numberLong").toString + "|" + recordTuple._2.toString.split("").filter(x => (x >= "0" && x <= "9") || x == ".").mkString(""), List(recordTuple._3.asInstanceOf[Map[String, Double]]("1"))))
    val ds2 = stream1.map(record => parse(record.value).values.asInstanceOf[Map[String, Any]]).map(record => (record("creationTime"), record("pollId"), record("kpiValues"))).map(recordTuple => (recordTuple._1.asInstanceOf[Map[String, Long]]("$numberLong").toString + "|" + recordTuple._2.toString.split("").filter(x => (x >= "0" && x <= "9") || x == ".").mkString(""), List(recordTuple._3.asInstanceOf[Map[String, Double]]("1"))))
    val ds3 = stream2.map(record => parse(record.value).values.asInstanceOf[Map[String, Any]]).map(record => (record("creationTime"), record("pollId"), record("kpiValues"))).map(recordTuple => (recordTuple._1.asInstanceOf[Map[String, Long]]("$numberLong").toString + "|" + recordTuple._2.toString.split("").filter(x => (x >= "0" && x <= "9") || x == ".").mkString(""), List(recordTuple._3.asInstanceOf[Map[String, Double]]("1"))))
    val ds4 = stream3.map(record => parse(record.value).values.asInstanceOf[Map[String, Any]]).map(record => (record("creationTime"), record("pollId"), record("kpiValues"))).map(recordTuple => (recordTuple._1.asInstanceOf[Map[String, Long]]("$numberLong").toString + "|" + recordTuple._2.toString.split("").filter(x => (x >= "0" && x <= "9") || x == ".").mkString(""), List(recordTuple._3.asInstanceOf[Map[String, Double]]("1"), recordTuple._3.asInstanceOf[Map[String, Double]]("2"), recordTuple._3.asInstanceOf[Map[String, Double]]("3"))))

    val j1 = ds1.fullOuterJoin(ds2).map(x => (x._1, x._2._1.getOrElse(Nil) ::: x._2._2.getOrElse(Nil)))
    val j2 = ds3.fullOuterJoin(j1).map(x => (x._1, x._2._1.getOrElse(Nil) ::: x._2._2.getOrElse(Nil)))
    val j3 = ds4.fullOuterJoin(j2).map(x => (x._1, x._2._1.getOrElse(Nil) ::: x._2._2.getOrElse(Nil)))

    val kafkaConf = new Properties()
    kafkaConf.put("bootstrap.servers", "localhost:9092")
    kafkaConf.put("key.serializer", "org.apache.kafka.common.serialization.StringSerializer")
    kafkaConf.put("value.serializer", "org.apache.kafka.common.serialization.StringSerializer")

    j3.foreachRDD(rdd => rdd.foreach(
      r => if(r._2.size == 6){
        val producer = new KafkaProducer[String, String](kafkaConf)
        producer.send(new ProducerRecord[String, String](kafkaSink, r.toString()))
        producer.close()
      }
    ))
    ssc.start()
    ssc.awaitTermination()
  }
}
