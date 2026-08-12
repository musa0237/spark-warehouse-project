package bronze

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions.{current_timestamp, lit}
import config.AppConfig
import schema.{
    CustomerSchema, ProductSchema, SalesSchema, 
    CustAz12Schema, LocA101Schema, PxCatSchema
}



object BronzeLoader {

    def loadCustomersInfo(spark: SparkSession): Unit = {

        println("Inserting Data Into: bronze.crm_cust_info")

        val custInfoDF = spark.read
            .option("header", "true")
            .schema(CustomerSchema.schema)
            .csv(AppConfig.custInfoPath)

        val custInfoPlusDF = custInfoDF
            .withColumn("ingestion_timestamp", current_timestamp()) 
            .withColumn("source", lit(AppConfig.custInfoPath))

        custInfoPlusDF.show(5)
        custInfoPlusDF.write
            .mode("overwrite")
            .jdbc(AppConfig.jdbcUrl, "bronze.crm_cust_info", AppConfig.jdbcProperties())

        println("Bronze Customers' info ingestion completed.")
    }

    def loadProductsInfo(spark: SparkSession): Unit = {

        println("Inserting Data Into: bronze.crm_prd_info")

        val prodInfoDF = spark.read
            .option("header", "true")
            .schema(ProductSchema.schema)
            .csv(AppConfig.prodInfoPath)

        val prodInfoPlusDF = prodInfoDF
            .withColumn("ingestion_timestamp", current_timestamp()) 
            .withColumn("source", lit(AppConfig.prodInfoPath))

        prodInfoPlusDF.show(5)
        prodInfoPlusDF.write
            .mode("overwrite")
            .jdbc(AppConfig.jdbcUrl, "bronze.crm_prd_info", AppConfig.jdbcProperties())

        println("Bronze Products' info ingestion completed.")
    }


    def loadSalesDetails(spark: SparkSession): Unit = {

        println("Inserting Data Into: bronze.crm_sales_details")

        val salesDetailsDF = spark.read
            .option("header", "true")
            .schema(SalesSchema.schema)
            .csv(AppConfig.salesDetailsPath)

        val salesDetailsPlusDF = salesDetailsDF
            .withColumn("ingestion_timestamp", current_timestamp()) 
            .withColumn("source", lit(AppConfig.salesDetailsPath))

        salesDetailsPlusDF.show(5)
        salesDetailsPlusDF.write
            .mode("overwrite")
            .jdbc(AppConfig.jdbcUrl, "bronze.crm_sales_details", AppConfig.jdbcProperties())

        println("Bronze Sales details ingestion completed.")
    }

    def loadCustAdditionalInfo(spark: SparkSession): Unit = {

        println("Inserting Data Into: bronze.erp_cust_az12")

        val custAz12DF = spark.read
            .option("header", "true")
            .schema(CustAz12Schema.schema)
            .csv(AppConfig.custAz12Path)

        val custAz12PlusDF = custAz12DF
            .withColumn("ingestion_timestamp", current_timestamp()) 
            .withColumn("source", lit(AppConfig.custAz12Path))

        custAz12PlusDF.show(5)
        custAz12PlusDF.write
            .mode("overwrite")
            .jdbc(AppConfig.jdbcUrl, "bronze.erp_cust_az12", AppConfig.jdbcProperties())

        println("Bronze Customer additional info ingestion completed.")
    }

    def loadLocAddress(spark: SparkSession): Unit = {

        println("Inserting Data Into: bronze.erp_loc_a101")

        val locA101DF = spark.read
            .option("header", "true")
            .schema(LocA101Schema.schema)
            .csv(AppConfig.locA101Path)

        val locA101PlusDF = locA101DF
            .withColumn("ingestion_timestamp", current_timestamp()) 
            .withColumn("source", lit(AppConfig.locA101Path))

        locA101PlusDF.show(5)
        locA101PlusDF.write
            .mode("overwrite")
            .jdbc(AppConfig.jdbcUrl, "bronze.erp_loc_a101", AppConfig.jdbcProperties())

        println("Bronze Customer Country Location ingestion completed.")
    }

    def loadProductsCategory(spark: SparkSession): Unit = {

        println("Inserting Data Into: bronze.erp_px_cat_g1v2")

        val pxCatG1V2DF = spark.read
            .option("header", "true")
            .schema(PxCatSchema.schema)
            .csv(AppConfig.pxCatG1v2Path)

        val pxCatG1V2PlusDF = pxCatG1V2DF
            .withColumn("ingestion_timestamp", current_timestamp()) 
            .withColumn("source", lit(AppConfig.pxCatG1v2Path))

        pxCatG1V2PlusDF.show(5)
        pxCatG1V2PlusDF.write
            .mode("overwrite")
            .jdbc(AppConfig.jdbcUrl, "bronze.erp_px_cat_g1v2", AppConfig.jdbcProperties())

        println("Bronze Product Category ingestion completed.")
    }
}





