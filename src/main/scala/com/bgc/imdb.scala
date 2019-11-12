package com.bgc

import org.apache.spark.sql.functions._
import org.apache.spark.sql.expressions.Window

/**
  * Created by jnayak on 11/11/2019.
  */
  class Main{
  def getDfs (namesFile: String, titlesFile: String, ratingsFile: String) = {

    // creating the imdb data into dataframes
    val df = new LoadData

    var names = df.readData("names", namesFile, 32)
    var titles = df.readData("basics", titlesFile, 32)
    var ratings = df.readData("ratings", ratingsFile, 32)


    // Trnasforming and replacing characters in movie id
    names = names.withColumn("movieId", regexp_replace(names("nconst"), "nm", ""))
    titles = titles.withColumn("movieId", regexp_replace(titles("tconst"), "tt", ""))
    ratings = ratings.withColumn("movieId", regexp_replace(ratings("tconst"), "tt", ""))

    // Getting average numVotes for all the movies
    val numVotesAvg = ratings.agg(avg("numVotes")).select("avg(numVotes)").as("numVotesAvg").collect()(0).getDouble(0)
    print(numVotesAvg)

    // Getting the movies with minimumm 50 votes or more
    val topMovies = titles.join(ratings, "movieId").filter(col("numVotes") >= 50)
      .select("movieId", "primaryTitle", "numVotes", "averageRating")
      .withColumn("votesRating", (col("numVotes") / numVotesAvg) * col("averageRating"))
    topMovies.show()

    // Getting the top20 movies
    val wSpec1 = Window.orderBy("votesRating")
    val top20Movies = topMovies.withColumn("top20Movies", rank.over(wSpec1)).limit(20)
    top20Movies.show()

    // Getting creaditor names for top 20 movies
    val top20MoviesCreditors = top20Movies.join(names, "movieId").select("movieId", "primaryTitle", "primaryName", "numVotes", "averageRating")
    top20MoviesCreditors.show()
    (top20Movies, top20MoviesCreditors)
  }
}