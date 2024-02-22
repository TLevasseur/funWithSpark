package edu.fun.spark

import org.apache.spark.sql.{Encoder, Encoders, SparkSession}

import scala.language.postfixOps

object Top10 {

  def main(args: Array[String]) {
    val spark = SparkSession.builder.appName("Top10").getOrCreate()
    val file = spark.read.textFile(args(0)).cache()
    implicit val gameEncoder: Encoder[Game] = Encoders.bean[Game](classOf[Game])
    val result = file
      .map(s => Game.fromColumns(s.split(",")))
      .rdd.groupBy(level)
      .flatMapValues(
        _.groupBy(_.opening_name)
          .mapValues(_.size)
          .foldLeft(Seq(): Seq[(String, Int)])((acc, value) => addIfTop10(acc, value))
      ).collect()
    result.foreach(v => println(s"${v._1} won ${v._2}"))
    spark.stop()
  }

  private def addIfTop10(seq: Seq[(String, Int)], value: (String, Int)): Seq[(String, Int)] = {
    if (seq.size < 10)
      seq :+ value
    else
      (seq :+ value).sorted.take(10)
  }

  private def level(game: Game) = {
    if (game.white_rating > 1700 && game.black_rating > 1700) {
      "Good"
    } else {
      "OK"
    }
  }
}