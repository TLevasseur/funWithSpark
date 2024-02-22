package edu.fun.spark


case class Game(id: String, rated: Boolean, turns: Int, victory_status: String, winner: String, increment_code: String, white_id: String, white_rating: Int, black_id: String, black_rating: Int, allMoves: String, opening_eco: String, opening_name: String, opening_ply: Int) extends Serializable

object Game {
  def fromColumns(columns: Array[String]) = new Game(parseText(columns, 0), parseBoolean(columns, 1), parseInt(columns, 4), parseText(columns, 5), parseText(columns, 6), parseText(columns, 7), parseText(columns, 8), parseInt(columns, 9), parseText(columns, 10), parseInt(columns, 11), parseText(columns, 12), parseText(columns, 13), parseText(columns, 14), parseInt(columns, 15))

  private def parseText(columns: Array[String], x: Int) = columns(x)

  private def parseInt(columns: Array[String], x: Int) = columns(x).toInt

  private def parseBoolean(columns: Array[String], x: Int) = columns(x).toBoolean

}