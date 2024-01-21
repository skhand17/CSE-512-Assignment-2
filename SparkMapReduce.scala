
import org.apache.log4j.{Level, Logger}
import org.apache.spark.sql.{SaveMode, SparkSession}
import org.apache.spark.sql.functions.split
import org.apache.spark.sql.functions._
import org.apache.spark.sql.DataFrame

object SparkMapReduce {

  Logger.getLogger("org.spark_project").setLevel(Level.WARN)
  Logger.getLogger("org.apache").setLevel(Level.WARN)
  Logger.getLogger("akka").setLevel(Level.WARN)
  Logger.getLogger("com").setLevel(Level.WARN)

  def runMapReduce(spark: SparkSession, pointPath: String, rectanglePath: String): DataFrame = {
    var pointDf = spark.read.format("csv").option("delimiter", ",").option("header", "false").load(pointPath);
    pointDf = pointDf.toDF()
    pointDf.createOrReplaceTempView("points")

    pointDf = spark.sql("select ST_Point(cast(points._c0 as Decimal(24,20)),cast(points._c1 as Decimal(24,20))) as point from points")
    pointDf.createOrReplaceTempView("pointsDf")
    pointDf.show()

    var rectangleDf = spark.read.format("csv").option("delimiter", ",").option("header", "false").load(rectanglePath);
    rectangleDf = rectangleDf.toDF()
    rectangleDf.createOrReplaceTempView("rectangles")

    rectangleDf = spark.sql("select ST_PolygonFromEnvelope(cast(rectangles._c0 as Decimal(24,20)),cast(rectangles._c1 as Decimal(24,20)), cast(rectangles._c2 as Decimal(24,20)), cast(rectangles._c3 as Decimal(24,20))) as rectangle from rectangles")
    rectangleDf.createOrReplaceTempView("rectanglesDf")
    rectangleDf.show()

    val joinDf = spark.sql("select rectanglesDf.rectangle as rectangle, pointsDf.point as point from rectanglesDf, pointsDf where ST_Contains(rectanglesDf.rectangle, pointsDf.point)")
    joinDf.createOrReplaceTempView("joinDf")
    joinDf.show()

    import spark.implicits._
    val joinRdd = joinDf.rdd

    // You need to complete this part
    var resultOfTheOperation = joinRdd.map(m=>(m(0).toString,1))
    var temporary = resultOfTheOperation.reduceByKey((c,d)=>c+d).sortBy(x=>x._2)
    var output = temporary.map(k => (k._2)).toDF().coalesce(1)
    return output
  }

}
