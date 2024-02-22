package edu.fun.spark

import edu.fun.spark.Player._
import org.apache.spark.sql.{Encoder, Encoders, SparkSession}

object David {
  def main(args: Array[String]) {
    val spark = SparkSession.builder.appName("David").getOrCreate()
    val file = spark.read.textFile(args(0)).cache()
    implicit val gameEncoder: Encoder[Game] = Encoders.bean[Game](classOf[Game])
    val result = file
      .map(s => Game.fromColumns(s.split(",")))
      .rdd.groupBy(davidVsGoliath)
      .mapValues(_.size)
      .reduceByKey(_ + _).collect()
    result.foreach(v => println(s"${v._1} won ${v._2}"))
    spark.stop()
  }

  private def davidVsGoliath(game: Game) = {
    if (goliathWon(game)) {
      "Goliath"
    } else {
      "David"
    }
  }

  private def goliathWon(value: Game): Boolean = (value.black_rating > value.white_rating && BLACK.getValue.equals(value.winner)) || (value.black_rating < value.white_rating && WHITE.getValue.equals(value.winner))
}
