import org.apache.spark.sql.SparkSession

import config.AppConfig
import database.DatabaseInitializer
import bronze.BronzeLoader
import silver.SilverTransformer
import gold.GoldTransformer



object WareHouseApp {

    def main(args: Array[String]): Unit = {

        val spark = SparkSession.builder
            .appName(AppConfig.applicationName)
            .master(AppConfig.sparkMaster)
            .getOrCreate()

        try {
            println()
            println("-----  Spark Data Warehouse Pipeline  -----")
            println()

            // 1. Initialize database
            println("Initializing database...")
            DatabaseInitializer.initialize()

            // 2. Bronze Layer
            println()
            println("*****  Loading Bronze Layer  *****")
            println("-----  Loading CRM Tables (Bronze) -----")
            println()
            BronzeLoader.loadCustomersInfo(spark)
            BronzeLoader.loadProductsInfo(spark)
            BronzeLoader.loadSalesDetails(spark)
            println()
            println("-----  Loading ERP Tables (Bronze) -----")
            println()
            BronzeLoader.loadCustAdditionalInfo(spark)
            BronzeLoader.loadLocAddress(spark)
            BronzeLoader.loadProductsCategory(spark)

            // 3. Silver Layer
            println()
            println("*****  Loading Silver Layer  *****")
            println("-----  Loading CRM Tables (Silver) -----")
            SilverTransformer.transformCustomersInfo(spark)
            SilverTransformer.transformProductsInfo(spark)
            SilverTransformer.transformSalesDetails(spark)
            println("-----  Loading ERP Tables (Silver) -----")
            SilverTransformer.transformCustAdditionalInfo(spark)
            SilverTransformer.transformLocAddress(spark)
            SilverTransformer.transformProductsCategory(spark)

            // 4. Gold Layer
            // println()
            // println("*****  Loading Gold Layer  *****")
            // GoldTransformer.createCustomerSummary(spark)
            

            // Pipeline completed
            println()
            println("-----  Pipeline completed successfully  -----")
        }
        finally {
            spark.stop()
        }
    }
}

