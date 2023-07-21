import org.apache.log4j.{Level, Logger}
import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

object WordCount {

  def main(arr: Array[String]): Unit = {
    //To avoid the multiple info logs in console (only for local testing)
    Logger.getLogger("org").setLevel(Level.ERROR)

    //Create SparkContext
    val sparkConf = new SparkConf()
    sparkConf.setMaster("local")
    sparkConf.setAppName("Word_Count")

    val spark = new SparkContext(sparkConf)

    //Read the text file
    val sourceRDD: RDD[String] = spark.textFile("src/main/resources/sample_text.txt")

    //Extracting only the words
    val extractedRDD: RDD[String] = sourceRDD.flatMap(line => line.split(" "))

    //Add count to the each word
    val mappedCountRDD: RDD[(String, Int)] = extractedRDD.map(word => (word, 1))

    //Taking the total count
    val finalCountWordsRDD: RDD[(String, Int)] = mappedCountRDD.reduceByKey((value1, value2) => value1 + value2)

    //Printing the final RDD
    finalCountWordsRDD.foreach(println)

  }
}
