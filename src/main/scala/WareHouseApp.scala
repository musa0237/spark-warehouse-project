import org.apache.spark.sql.SparkSession

import config.AppConfig
import database.{DatabaseInitializer, JdbcDataAccess}
import bronze.BronzeLoader
import silver.SilverTransformer
import gold.GoldTransformer
import quality.GoldQualityChecks
import quality.SilverQualityChecks



object WareHouseApp {

    def main(args: Array[String]): Unit = {

        val spark = SparkSession.builder
            .appName(AppConfig.applicationName)
            .master(AppConfig.sparkMaster)
            .getOrCreate()

        try {
            println()
            println("============================================================")
            println("=============  Spark Data Warehouse Pipeline  ==============")
            println("============================================================")
            println()

            // Initialize database
            println("Initializing database...")
            DatabaseInitializer.initialize()
            val db = new JdbcDataAccess(spark)

            // Bronze Layer
            println()
            val bronzeLoader = new BronzeLoader(spark, db)
            bronzeLoader.run()

            // Silver Layer
            println()
            val silverTransformer = new SilverTransformer(spark, db)
            silverTransformer.run()

            val silverQualityChecks = new SilverQualityChecks(spark, db)
            silverQualityChecks.run()

            // Gold Layer
            println()
            val goldTransformer = new GoldTransformer(spark, db)
            goldTransformer.run()

            val goldQualityChecks = new GoldQualityChecks(spark, db)
            goldQualityChecks.run()

            // Pipeline completed
            println()
            println("============================================================")
            println("============  Pipeline completed successfully  =============")
            println("============================================================")
        }
        finally {
            spark.stop()
        }
    }
}



