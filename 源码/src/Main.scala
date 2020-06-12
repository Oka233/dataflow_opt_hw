import java.util.{Properties, UUID}
import org.apache.flink.api.common.serialization.SimpleStringSchema
import org.apache.flink.streaming.api.scala.StreamExecutionEnvironment
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer010
import org.apache.kafka.clients.producer.{KafkaProducer, ProducerRecord}
import org.apache.flink.api.scala._

object Main {
  def main(args: Array[String]): Unit ={
    val accessKey = ""
    val secretKey = ""
    val endpoint = "scuts3.depts.bingosoft.net:29999"
    val bucket = "shaoditong"
    val key = "daas.txt"
    val s3Utils = new S3Utils(bucket, accessKey, secretKey, endpoint)
    val content = s3Utils.read(bucket, key)
    producer(content)
    consumer(content, s3Utils)
  }

  def producer(s3Content: String) : Unit = {
    val props = new Properties
    props.put("bootstrap.servers", "bigdata35.depts.bingosoft.net:29035,bigdata36.depts.bingosoft.net:29036,bigdata37.depts.bingosoft.net:29037")
    props.put("acks", "all")
    props.put("key.serializer", "org.apache.kafka.common.serialization.StringSerializer")
    props.put("value.serializer", "org.apache.kafka.common.serialization.StringSerializer")
    val producer = new KafkaProducer[String, String](props)
    val data = s3Content.split("\n")
    for (s <- data) {
      if (!s.trim.isEmpty) {
        val record = new ProducerRecord[String, String]("sdt_assignment", null, s)
        println("开始生产数据：" + s)
        producer.send(record)
      }
    }
    producer.flush()
    producer.close()
  }

  def consumer(content: String, reader: S3Utils): Unit =   {
    val txtMap = classify(content)
    reader.write(txtMap)
    val env = StreamExecutionEnvironment.getExecutionEnvironment
    val kafkaProperties = new Properties()
    kafkaProperties.put("bootstrap.servers", "bigdata35.depts.bingosoft.net:29035,bigdata36.depts.bingosoft.net:29036,bigdata37.depts.bingosoft.net:29037")
    kafkaProperties.put("group.id", UUID.randomUUID().toString)
    kafkaProperties.put("auto.offset.reset", "earliest")
    kafkaProperties.put("key.deserializer", "org.apache.kafka.common.serialization.StringDeserializer")
    kafkaProperties.put("value.deserializer", "org.apache.kafka.common.serialization.StringDeserializer")
    val kafkaConsumer = new FlinkKafkaConsumer010[String]( "sdt_assignment", new SimpleStringSchema, kafkaProperties)
    kafkaConsumer.setCommitOffsetsOnCheckpoints(true)
    val inputKafkaStream = env.addSource(kafkaConsumer)
    inputKafkaStream.map(x => {
      val pattern = """.+destination":(\s*)"(.+?)".+""".r
      val dest = x match {
        case pattern(_, dest) => s"$dest"
      }
      println("进行分类：" + dest + " ------->"  + x)
    }
    )
    env.execute()
  }

  def classify(content: String) : scala.collection.mutable.Map[String, String] ={
    val record = content.split("\n")
    var states = scala.collection.mutable.Map[String,Array[Int]]()
    for(i <- 0 to (record.length - 1)){
      val pattern = """.+destination":(\s*)"(.+?)".+""".r
      val destination = record(i) match {
        case pattern(_, dest) => s"$dest"
      }
      if(states.contains(destination)) {
        var x = states(destination) :+ i
        states(destination)= x
      }
      else {
        states = states + (destination -> Array(i))
      }
    }
    var result = scala.collection.mutable.Map[String,String]()
    states.keys.foreach{i =>
      val x = states(i)
      var str = ""
      for(x <- x){
        str += "\n" + record(x)
      }
      result = result + (i -> str)
    }
    return result
  }
}