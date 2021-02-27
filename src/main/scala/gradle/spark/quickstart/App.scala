/*
 * This Scala source file was generated by the Gradle 'init' task.
 */
package gradle.spark.quickstart

import org.apache.spark.sql.{DataFrame, Dataset, SparkSession}

object App {
  def main(args: Array[String]): Unit = {
    some().show()
  }

  def some(): Dataset[MovieWithSingleGenre] = {
    val spark: SparkSession = sparkFactory
    import spark.implicits._
    val source = spark.read
      .option("header", true)
      .option("inferSchema", true)
      .csv("/home/dma/projects/hadoop-workshop/ml-latest/movies.csv")
      .as[Movie]
    pipeline(source)(spark)

  }

  def pipeline(source: Dataset[Movie])(implicit spark: SparkSession) = {
    import spark.implicits._
    source
      .filter(_.title.matches("""^"?.*\(\d{4}\)\s*"?$"""))
      .map(m => {
        val lastOpen = m.title.lastIndexOf("(")
        val year = m.title.substring(lastOpen + 1, lastOpen + 5).toInt
        MovieWithYear(m.movieId, m.title.replaceAll("""\(\d{4}\)\s*"?$""", "").trim, m.genres, year)
      })
      .map(m => MovieWithGenresAndYear(m.movieId, m.title, m.genres.split("\\|"), m.year))
      .flatMap(movie => movie.genres.map(genre => MovieWithSingleGenre(movie.movieId, movie.title, genre, movie.year)))
      .filter(_.genre != "(no genres listed)")
  }

  private def sparkFactory = {
    val spark = SparkSession
      .builder()
      .appName("test")
      .master("local")
      .getOrCreate()
    spark
  }
}

case class Movie(movieId: Long, title: String, genres: String)

case class MovieWithYear(movieId: Long, title: String, genres: String, year: Integer)

case class MovieWithGenresAndYear(movieId: Long, title: String, genres: Array[String], year: Integer)

case class MovieWithSingleGenre(movieId: Long, title: String, genre: String, year: Integer)

case class MovieExploded(movieId: Long, title: String, genres: Array[String])

case class MovieAggregate(year: Int, count: Long)

