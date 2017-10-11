import org.apache.spark.sql.{DataFrame, Row, SQLContext, SparkSession}
import org.neo4j.driver.v1._
import org.apache.spark.{SparkConf, SparkContext, sql}
import org.neo4j.driver.v1.Values._
import org.neo4j.driver.v1.AuthTokens
import org.neo4j.driver.v1.GraphDatabase
import org.neo4j.driver.v1.StatementResult

object MoviesToNeo {
  def main(args: Array[String]): Unit = {
    val conf = new SparkConf().setAppName("Tal").setMaster("local[2]")
    val sc = new SparkContext(conf)
    val sparkSession = SparkSession.builder
      .config(conf = conf)
      .appName("spark session example")
      .getOrCreate()

    val driver = GraphDatabase.driver("bolt://localhost:7687", AuthTokens.basic("neo4j", "321321"))
    val session = driver.session

    // Assumptions:
    // 1. I dont need all the data that is given, hence not saving all of it.
    saveMovieToNeo(sparkSession, session)
    saveRatingsToNeo(sparkSession, session)

    session.close()
    sc.stop()
  }

  def saveRatingsToNeo(sparkSession:SparkSession, session: Session): Unit ={

    val ratingsPath = "Resources/ratings.csv"
    val data = sparkSession.read.option("header","true").csv(ratingsPath)

    data
      .collect()
      .foreach(x => {
        session.run("CREATE INDEX ON :USER(userId)")
        session.run("MATCH (m:Movie) WHERE m.movieId={movieToFind}" +
          "MERGE (u:User {userId: {userId}})" +
          "CREATE (u)-[:Rated {rating: {rating}, timestamp: {timestamp}}]->(m)",
          parameters("movieToFind",value(x(1).toString.toInt),"userId",value(x(0).toString.toInt), "rating", value(x(2).toString.toDouble), "timestamp", value(x(3).toString.toLong)))
      })
  }

  def saveMovieToNeo(sparkSession:SparkSession, session: Session): Unit ={

    val moviePath = "Resources/movies.csv"
    val linksPath = "Resources/links.csv"
    val moviesDF = sparkSession.read.option("header","true").csv(moviePath)
    val linksDF = sparkSession.read.option("header","true").csv(linksPath)
    val data = moviesDF.join(linksDF, "movieId")

    data
      .collect()
      .foreach(x => {

        // Creating Index on id
        session.run("CREATE INDEX ON :Movie(movieId)")

        // Saving Genres as different nodes, connected to movies.
        val genres= x(2).toString.split('|')
        val merges = genres.map(g => "MERGE (b"+genres.indexOf(g)+":Genre{genreName:\""+g+"\"})").mkString(" ")
        val creates = genres.map(g=> s"(a)-[:OfGenre]->(b${genres.indexOf(g)})").mkString(", ")

        session.run(merges +
          " CREATE (a:Movie {movieId: {movieId}, title: {title}, imdbId: {imdbId}, tmdbId: {tmdbId}}), " + creates,
          parameters("movieId",value(x(0).toString.toInt),"title",value(x(1)), "imdbId", value(x(3)), "tmdbId", value(x(4))))

      })
  }
}
