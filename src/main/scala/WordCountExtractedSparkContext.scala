import org.apache.spark.rdd.RDD
import org.apache.spark.sql.SparkSession

object WordCountExtractedSparkContext {

  def main(args: Array[String]): Unit = {
    val sparkSession = SparkSession.builder.master("local").appName("WordCountApp").getOrCreate()

    val sparkContext = sparkSession.sparkContext

    //Read the text file
    val sourceRDD: RDD[String] = sparkContext.textFile("src/main/resources/sample_text.txt")

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
