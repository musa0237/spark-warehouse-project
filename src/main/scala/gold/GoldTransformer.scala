package gold
import org.apache.spark.sql.SparkSession
import config.AppConfig



object GoldTransformer {

    def createCustomerSummary(spark: SparkSession): Unit = {

        println("Starting Gold customer summary...")

        val silverCustomers = spark.read
            .jdbc(AppConfig.jdbcUrl, "silver.crm_cust_info", AppConfig.jdbcProperties())

        val customerSummary = silverCustomers
            .groupBy("cst_gndr")
            .count()
            .withColumnRenamed("count", "gender_count")

        customerSummary.show(5)
        customerSummary.write
            .mode("overwrite")
            .jdbc(AppConfig.jdbcUrl, "gold.customer_summary", AppConfig.jdbcProperties())

        println("Gold customer summary completed.")
    }
}

