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

object iot_lab3 {
  def main(args: Array[String]): Unit = {
    val conf = new SparkConf().setMaster("local[5]").setAppName("NetworkWordCount")
    val ssc = new StreamingContext(conf, Seconds(1))

    val kafkaParams1 = Map[String, Object](
      "bootstrap.servers" -> "localhost:9092",
      "key.deserializer" -> classOf[StringDeserializer],
      "value.deserializer" -> classOf[StringDeserializer],
      "group.id" -> "stream1",
      "auto.offset.reset" -> "earliest",
      "enable.auto.commit" -> (false: java.lang.Boolean)
    )
    val kafkaParams2 = Map[String, Object](
      "bootstrap.servers" -> "localhost:9092",
      "key.deserializer" -> classOf[StringDeserializer],
      "value.deserializer" -> classOf[StringDeserializer],
      "group.id" -> "stream2",
      "auto.offset.reset" -> "earliest",
      "enable.auto.commit" -> (false: java.lang.Boolean)
    )
    val kafkaParams3 = Map[String, Object](
      "bootstrap.servers" -> "localhost:9092",
      "key.deserializer" -> classOf[StringDeserializer],
      "value.deserializer" -> classOf[StringDeserializer],
      "group.id" -> "stream3",
      "auto.offset.reset" -> "earliest",
      "enable.auto.commit" -> (false: java.lang.Boolean)
    )
    val kafkaParams4 = Map[String, Object](
      "bootstrap.servers" -> "localhost:9092",
      "key.deserializer" -> classOf[StringDeserializer],
      "value.deserializer" -> classOf[StringDeserializer],
      "group.id" -> "stream4",
      "auto.offset.reset" -> "earliest",
      "enable.auto.commit" -> (false: java.lang.Boolean)
    )

    val topics1 = Array("mem1")
    val topics2 = Array("tem1")
    val topics3 = Array("cpu1")
    val topics4 = Array("bNg1")
    val kafkaSink = "out1"

    val stream1 = KafkaUtils.createDirectStream[String, String](ssc, PreferConsistent, Subscribe[String, String](topics1, kafkaParams1))
    val stream2 = KafkaUtils.createDirectStream[String, String](ssc, PreferConsistent, Subscribe[String, String](topics2, kafkaParams2))
    val stream3 = KafkaUtils.createDirectStream[String, String](ssc, PreferConsistent, Subscribe[String, String](topics3, kafkaParams3))
    val stream4 = KafkaUtils.createDirectStream[String, String](ssc, PreferConsistent, Subscribe[String, String](topics4, kafkaParams4))

    val ds1 = stream1.map(record => parse(record.value).values.asInstanceOf[Map[String, Any]]).map(record => (record("creationTime"), record("pollId"), record("kpiValues"))).map(recordTuple => (recordTuple._1.asInstanceOf[Map[String, Long]]("$numberLong").toString + "|" + recordTuple._2.toString.split("").filter(x => (x >= "0" && x <= "9") || x == ".").mkString(""), List(recordTuple._3.asInstanceOf[Map[String, Double]]("1"))))
    val ds2 = stream2.map(record => parse(record.value).values.asInstanceOf[Map[String, Any]]).map(record => (record("creationTime"), record("pollId"), record("kpiValues"))).map(recordTuple => (recordTuple._1.asInstanceOf[Map[String, Long]]("$numberLong").toString + "|" + recordTuple._2.toString.split("").filter(x => (x >= "0" && x <= "9") || x == ".").mkString(""), List(recordTuple._3.asInstanceOf[Map[String, Double]]("1"))))
    val ds3 = stream3.map(record => parse(record.value).values.asInstanceOf[Map[String, Any]]).map(record => (record("creationTime"), record("pollId"), record("kpiValues"))).map(recordTuple => (recordTuple._1.asInstanceOf[Map[String, Long]]("$numberLong").toString + "|" + recordTuple._2.toString.split("").filter(x => (x >= "0" && x <= "9") || x == ".").mkString(""), List(recordTuple._3.asInstanceOf[Map[String, Double]]("1"))))
    val ds4 = stream4.map(record => parse(record.value).values.asInstanceOf[Map[String, Any]]).map(record => (record("creationTime"), record("pollId"), record("kpiValues"))).map(recordTuple => (recordTuple._1.asInstanceOf[Map[String, Long]]("$numberLong").toString + "|" + recordTuple._2.toString.split("").filter(x => (x >= "0" && x <= "9") || x == ".").mkString(""), List(recordTuple._3.asInstanceOf[Map[String, Double]]("1"), recordTuple._3.asInstanceOf[Map[String, Double]]("2"), recordTuple._3.asInstanceOf[Map[String, Double]]("3"))))

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
        producer.send(new ProducerRecord[String, String](kafkaSink, r._2.toString()))
        producer.close()
      }
    ))
    ssc.start()
    ssc.awaitTermination()
  }
}
