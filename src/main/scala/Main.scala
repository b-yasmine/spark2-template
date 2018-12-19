import spark_helpers.SessionBuilder

object Main {
  def main(args: Array[String]): Unit = {

    val spark = SessionBuilder.buildSession()

    val sparkVersion = spark.version
    println(s"Spark Version: $sparkVersion")

    //sql_practice.examples.sample()
    //sql_practice.exercice1.exec1()
    //sql_practice.exercice2.exec2()
    sql_practice.exercice3.exec3()

  }
}
