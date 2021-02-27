# gradle-spark-quickstart
Sample project with Gralde, Scala and Spark 3.0

# Quick prototyping with zeppelin
See :https://zeppelin.apache.org/docs/0.8.0/quickstart/install.html
Examples:
```
import org.apache.spark.sql.types.IntegerType

val df = spark.read
.option("header", true)
.option("inferSchema", true)
.csv("/home/dma/projects/hadoop-workshop/ml-latest/movies.csv")
.cache()

%

df
.filter($"title" rlike (""""?.*\(\d{4}\)\s*"?"""))
.withColumn("year", regexp_extract($"title", """\((\d{4})\)\s*"?""", 1).cast(IntegerType))
.withColumn("title", regexp_replace($"title", """\(\d{4}\)\s*"?""", ""))
.groupBy($"year")
.agg(count($"title"), $"year")
.createOrReplaceTempView("countOfMovies")


%spark.sql

SELECT `count(title)` as cnt, year
FROM countofmovies
WHERE year BETWEEN 1910 and 2016
ORDER BY year
%spark
import org.apache.spark.sql.types.{IntegerType, StringType}

case class Movie(movieId: Long, title: String, genres: String)

case class MovieWithYear(movieId: Long, title: String, genres: String, year: Integer)

case class MovieWithGenresAndYear(movieId: Long, title: String, genres: Array[String], year: Integer)

case class MovieWithSingleGenre(movieId: Long, title: String, genre: String, year: Integer)

case class MovieExploded(movieId: Long, title: String, genres: Array[String])

case class MovieAggregate(year: Int, count: Long)

import spark.implicits._ 

val source = spark.read
.option("header", true)
.option("inferSchema", true)
.csv("/home/dma/projects/hadoop-workshop/ml-latest/movies.csv")
.as[Movie]
.filter(_.title.matches("""^"?.*\(\d{4}\)\s*"?$"""))
.map(m => {
val lastOpen = m.title.lastIndexOf("(")
val year = m.title.substring(lastOpen + 1, lastOpen + 5).toInt
MovieWithYear(m.movieId, m.title.replaceAll("""\(\d{4}\)\s*"?$""", ""), m.genres, year)
})
.map(m => MovieWithGenresAndYear(m.movieId, m.title, m.genres.split("\\|"), m.year))
.flatMap(m => m.genres.map(g => MovieWithSingleGenre(m.movieId, m.title, g, m.year)))
.filter(_.genre != "(no genres listed)") 

%
source
.groupByKey(_.year)
.mapGroups((k, v) => (k, v.size))
.orderBy("_1")
.show() 

%spark
case class Rating(userId: Long, movieId: Long, rating: Double, timestamp: Long)

%spark
val ratings = spark.read
        .option("header", true)
.option("inferSchema", true)
.csv("/home/dma/projects/hadoop-workshop/ml-latest/ratings.csv")
.as[Rating]
.limit(10)
%spark

source.joinWith(ratings, source.col("movieId") === ratings.col("movieId"), "left")
        .filter(_._2 != null)
        .map {
            case (a, b) =>
                (a.title, b.rating)
        }
        .distinct()
        .limit(1)

%

```
