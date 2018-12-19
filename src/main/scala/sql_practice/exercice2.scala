package sql_practice

import org.apache.spark.sql.functions._
import spark_helpers.SessionBuilder

object exercice2 {

  def exec2(): Unit = {
    val spark = SessionBuilder.buildSession()
    import spark.implicits._

    val s07 = spark.read
      .option("sep","\t")
      .csv("data/input/sample_07")
      .select($"_c0".as("code"),
              $"_c1".as("description07"),
              $"_c2".as("total_emp07"),
              $"_c3".as("salary07"))

    val s08 = spark.read
      .option("sep","\t")
      .csv("data/input/sample_08")
      .select($"_c0".as("code"),
        $"_c1".as("description08"),
        $"_c2".as("total_emp08"),
        $"_c3".as("salary08"))

    val s0708 = s08.join(s07, Seq("code"))

    // Top salaries above 100k in 2007
    s07.select($"code", $"description07".as("description"), $"salary07".as("salary"))
      .where($"salary07">100000)
      .orderBy($"salary07".desc).show()

    // Salary growth from 2007-08 sorted
    s0708.select($"code", $"description08".as("descriprion"), $"salary07".as("old_salary"), $"salary08".as("new_salary"))
      .withColumn("salary_growth", ($"new_salary"-$"old_salary"))
      .withColumn("salary_growth_percentage", ($"salary_growth"/$"new_salary")*100)
      .where($"salary_growth" > 0)
      .orderBy($"salary_growth_percentage".desc)
      .show()

    // Job loss among top earning (above 100k)
    s0708.select($"code", $"description08".as("descriprion"), $"total_emp07".as("old_emp"), $"total_emp08".as("new_emp"))
      .withColumn("job_loss", ($"old_emp"-$"new_emp"))
      .withColumn("job_loss_percentage", ($"job_loss"/$"old_emp")*100)
      .where($"job_loss" > 0 && $"salary07" > 100000)
      .orderBy($"job_loss_percentage".desc)
      .show()

  }
}
