import org.apache.log4j.{Level, Logger}
import org.apache.spark.SparkContext

object FirstOwnerYamahaPowerBikes extends App {

  Logger.getLogger("org").setLevel(Level.ERROR)

  //1. Set the sparkContext
  private val sparkContext = new SparkContext("local", "FirstOwnerYamahaPowerBikes")

  //2. To read the csv file
  val sourceBikesRDD = sparkContext.textFile("src/main/resources/Used_Bikes.csv")

  //3. To apply the use case logics
  val splittedRDD = sourceBikesRDD.map(line => line.split(",").map(column => column.trim))

  val resultRDD = splittedRDD.filter(bike => bike(4).equalsIgnoreCase("First Owner")
    && bike(6).toDouble > 150 && bike(7).equalsIgnoreCase("Yamaha"))

  //4. Print the datasets
  resultRDD.map(bike => (bike(0), bike(4))).distinct().foreach(println)
  println("Count of the final Datasets " + resultRDD.count())
}
