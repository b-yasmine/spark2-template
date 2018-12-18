package sql_practice

import spark_helpers.SessionBuilder
import org.apache.spark.sql.functions._



object exercice1 {

  def exec1(): Unit ={
    val spark = SessionBuilder.buildSession()

    val demographieDF = spark.read
      .json("data/input/demographie_par_commune.json")

    // Population of France
    demographieDF.agg(sum("population").as("France Population")).show()

    // Highly populated departments (codes)
    val departPopDF = demographieDF.groupBy("departement")
      .agg(sum("population").as("total_population"))
      .orderBy(col("total_population").desc)

    departPopDF.show()

    // Highly populated departments (names, using join)
    val departementsDF = spark.read.text("data/input/departements.txt")
      .withColumn("name", split(col("value"), ","))
      .select(col("name")(0).as("name"),col("name")(1).as("departement"))

    departementsDF.join(departPopDF, Seq("departement")).show()
  }
}
