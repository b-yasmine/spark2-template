package sql_practice

import spark_helpers.SessionBuilder
import org.apache.spark.sql.functions._


object exercice3 {

  def exec3(): Unit ={
    val spark = SessionBuilder.buildSession()
    import spark.implicits._

    val toursDF = spark.read
      .option("multiline", true)
      .option("mode", "PERMISSIVE")
      .json("data/input/tours.json")

    toursDF.show

    // 1. unique levels of difficulties
    toursDF.select($"tourDifficulty".as("difficulties"))
      .groupBy("difficulties").count()
      .show()

    // 2. Min Max Avg of tour prices
    toursDF.agg(min("tourPrice").as("min_price"),
                max("tourPrice").as("max_price"),
                avg("tourPrice").as("average_price"))
      .show()

    // 3. Min Max Avg of tour prices for each level of difficulty
    toursDF.select($"tourDifficulty".as("difficulties"),$"tourPrice")
      .groupBy("difficulties")
      .agg(min("tourPrice").as("min_price"),
           max("tourPrice").as("max_price"),
           avg("tourPrice").as("average_price"))
      .show()

    // 4. Min Max Avg of tour prices and Min Max Avg of length for each level of difficulty
    toursDF.select($"tourDifficulty".as("difficulties"),$"tourPrice",$"tourLength")
      .groupBy("difficulties")
      .agg(min("tourPrice").as("min_price"),
           max("tourPrice").as("max_price"),
           avg("tourPrice").as("average_price"),
           min("tourLength").as("min_duration"),
           max("tourLength").as("max_duration"),
           avg("tourLength").as("average_duration"))
      .show()

    // 5. Top 10 tour Tags
    toursDF.select(explode($"tourTags").as("tourTag"))
      .groupBy("tourTag").count().as("count")
      .orderBy($"count".desc)
      .show(10)

    // 6. Relationship between Top 10 tour Tags and tourDifficulty
    toursDF.select(explode($"tourTags").as("tourTag"), $"tourDifficulty")
      .groupBy($"tourTag", $"tourDifficulty").count().as("count")
      .orderBy($"count".desc).
      show(10)

    // 7. Min Max Avg of price Relationship between Top 10 tour Tags and tourDifficulty
    toursDF.select(explode($"tourTags").as("tourTag"), $"tourDifficulty",$"tourPrice")
      .groupBy($"tourTag", $"tourDifficulty")
      .agg(min("tourPrice").as("min_price"),
        max("tourPrice").as("max_price"),
        avg("tourPrice").as("average_price"))
      .orderBy($"average_price".desc)
      .show()
  }

}
