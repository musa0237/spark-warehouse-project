package bronze

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions.{current_timestamp, lit}
import config.AppConfig
import schema.{
    CustomerSchema, ProductSchema, SalesSchema, 
    CustAz12Schema, LocA101Schema, PxCatSchema
}
import database.JdbcDataAccess



class BronzeLoader(spark: SparkSession, db: JdbcDataAccess) {

    private def loadCustomersInfo(): Unit = {

        println("Inserting Data Into: bronze.crm_cust_info")

        val custInfoDF = spark.read
            .option("header", "true")
            .schema(CustomerSchema.schema)
            .csv(AppConfig.custInfoPath)

        val custInfoPlusDF = custInfoDF
            .withColumn("ingestion_timestamp", current_timestamp()) 
            .withColumn("source", lit(AppConfig.custInfoPath))

        custInfoPlusDF.show(5)
        db.writeTable(custInfoPlusDF, "bronze.crm_cust_info")

        println("Bronze Customers' info ingestion completed.")
    }

    private def loadProductsInfo(): Unit = {

        println("Inserting Data Into: bronze.crm_prd_info")

        val prodInfoDF = spark.read
            .option("header", "true")
            .schema(ProductSchema.schema)
            .csv(AppConfig.prodInfoPath)

        val prodInfoPlusDF = prodInfoDF
            .withColumn("ingestion_timestamp", current_timestamp()) 
            .withColumn("source", lit(AppConfig.prodInfoPath))

        prodInfoPlusDF.show(5)
        db.writeTable(prodInfoPlusDF, "bronze.crm_prd_info")

        println("Bronze Products' info ingestion completed.")
    }


    private def loadSalesDetails(): Unit = {

        println("Inserting Data Into: bronze.crm_sales_details")

        val salesDetailsDF = spark.read
            .option("header", "true")
            .schema(SalesSchema.schema)
            .csv(AppConfig.salesDetailsPath)

        val salesDetailsPlusDF = salesDetailsDF
            .withColumn("ingestion_timestamp", current_timestamp()) 
            .withColumn("source", lit(AppConfig.salesDetailsPath))

        salesDetailsPlusDF.show(5)
        db.writeTable(salesDetailsPlusDF, "bronze.crm_sales_details")

        println("Bronze Sales details ingestion completed.")
    }

    private def loadCustAdditionalInfo(): Unit = {

        println("Inserting Data Into: bronze.erp_cust_az12")

        val custAz12DF = spark.read
            .option("header", "true")
            .schema(CustAz12Schema.schema)
            .csv(AppConfig.custAz12Path)

        val custAz12PlusDF = custAz12DF
            .withColumn("ingestion_timestamp", current_timestamp()) 
            .withColumn("source", lit(AppConfig.custAz12Path))

        custAz12PlusDF.show(5)
        db.writeTable(custAz12PlusDF, "bronze.erp_cust_az12")

        println("Bronze Customer additional info ingestion completed.")
    }

    private def loadLocAddress(): Unit = {

        println("Inserting Data Into: bronze.erp_loc_a101")

        val locA101DF = spark.read
            .option("header", "true")
            .schema(LocA101Schema.schema)
            .csv(AppConfig.locA101Path)

        val locA101PlusDF = locA101DF
            .withColumn("ingestion_timestamp", current_timestamp()) 
            .withColumn("source", lit(AppConfig.locA101Path))

        locA101PlusDF.show(5)
        db.writeTable(locA101PlusDF, "bronze.erp_loc_a101")

        println("Bronze Customer Country Location ingestion completed.")
    }

    private def loadProductsCategory(): Unit = {

        println("Inserting Data Into: bronze.erp_px_cat_g1v2")

        val pxCatG1V2DF = spark.read
            .option("header", "true")
            .schema(PxCatSchema.schema)
            .csv(AppConfig.pxCatG1v2Path)

        val pxCatG1V2PlusDF = pxCatG1V2DF
            .withColumn("ingestion_timestamp", current_timestamp()) 
            .withColumn("source", lit(AppConfig.pxCatG1v2Path))

        pxCatG1V2PlusDF.show(5)
        db.writeTable(pxCatG1V2PlusDF, "bronze.erp_px_cat_g1v2")

        println("Bronze Product Category ingestion completed.")
    }

    def run(): Unit = {
        println("============================================================")
        println("================= Starting Bonze Loading ===================")
        println("============================================================")
        println()
        println("-----  Loading CRM Tables (Bronze) -----")
        println()
        loadCustomersInfo()
        loadProductsInfo()
        loadSalesDetails()
        println()
        println("-----  Loading ERP Tables (Bronze) -----")
        println()
        loadCustAdditionalInfo()
        loadLocAddress()
        loadProductsCategory()
        println()
        println("============================================================")
        println("=============== Bronze Loading completed ===================")
        println("============================================================")
    }
}





