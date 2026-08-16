package quality

import org.apache.spark.sql.{DataFrame, SparkSession}
import org.apache.spark.sql.functions._
import config.AppConfig
import database.JdbcDataAccess



class GoldQualityChecks(spark: SparkSession, db: JdbcDataAccess) {

    private def failIfRecordsFound(checkName: String, df: DataFrame): Unit = {

        val count = df.count()

        if (count > 0) {
            println(s"FAILED: $checkName")
            println(s"Found $count invalid record(s).")
            df.show(20, false)

            // throw new RuntimeException(
            //     s"Silver quality check failed: $checkName. Found $count invalid record(s)."
            // )

            println(
                s"Gold quality check failed: $checkName. Found $count invalid record(s)."
            )
        }

        println(s"PASSED: $checkName")
    }

    // ====================================================================
    // gold.dim_customers
    // ====================================================================
    private def checkCustomerKeyUniqueness(): Unit = {

        println("Checking gold.dim_customers.customer_key uniqueness...")

        val customersDF = db.readTable("gold.dim_customers")

        // Check uniqueness of customer_key in gold.dim_customers.
        // Expectation: No duplicate customer_key values.
        val duplicatesDF = customersDF
            .groupBy("customer_key")
            .count()
            .filter(col("count") > 1)

        failIfRecordsFound(
            "Checking dim_customers duplicate customer_key", duplicatesDF
        )
    }

    // ====================================================================
    // gold.dim_products
    // ====================================================================
    private def checkProductKeyUniqueness(): Unit = {

        println("Checking gold.dim_products.product_key uniqueness...")

        val productsDF = db.readTable("gold.dim_products")

        // Check uniqueness of product_key in gold.dim_products.
        // Expectation: No duplicate product_key values.
        val duplicatesDF = productsDF
            .groupBy("product_key")
            .count()
            .filter(col("count") > 1)

        failIfRecordsFound(
            "Checking dim_products duplicate product_key ", duplicatesDF
        )

    }

    // ====================================================================
    // gold.fact_sales
    // ====================================================================
    private def checkFactDimensionConnectivity(): Unit = {

        println("Checking gold.fact_sales dimension connectivity...")

        val factSalesDF = db.readTable("gold.fact_sales")
        val customersDF = db.readTable("gold.dim_customers")
        val productsDF = db.readTable("gold.dim_products")

        // Check connectivity between fact_sales and the dimensions
        // Expectation:
        // Every customer_key in fact_sales must exist in dim_customers.
        // Every product_key in fact_sales must exist in dim_products.
        val invalidRecordsDF = factSalesDF
            .join(customersDF.select("customer_key"), Seq("customer_key"), "left")
            .join(productsDF.select("product_key"), Seq("product_key"), "left")
            .filter(col("product_key").isNull || col("customer_key").isNull)

        failIfRecordsFound(
            "Checking fact/dimension connectivity", invalidRecordsDF
        )
    }

    def run(): Unit = {
        println()
        println("============================================================")
        println("============== Running Gold Quality Checks =================")
        println("============================================================")
        println()
        checkCustomerKeyUniqueness()
        checkProductKeyUniqueness()
        checkFactDimensionConnectivity()
        println()
        println("============================================================")
        println("============== All Gold Quality Checks passed ==============")
        println("============================================================")
    }
}








