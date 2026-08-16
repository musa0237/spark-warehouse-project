package silver
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._
import org.apache.spark.sql.expressions.Window
import config.AppConfig
import database.JdbcDataAccess



class SilverTransformer(spark: SparkSession, db: JdbcDataAccess) {

    import spark.implicits._

    def transformCustomersInfo(): Unit = {

        println("Inserting Data Into: silver.crm_cust_info")

        val bronzeCustInfoDF = db.readTable("bronze.crm_cust_info")
        
        val windowSpec = Window.partitionBy("cst_id").orderBy($"cst_create_date".desc)

        val silverCustInfoDF = bronzeCustInfoDF
            .withColumn("cst_firstname", trim($"cst_firstname"))
            .withColumn("cst_lastname", trim($"cst_lastname"))
            .withColumn("cst_marital_status",
                when(upper(trim($"cst_marital_status")) === "S", "Single")
                .when(upper(trim($"cst_marital_status")) === "M", "Married")
                .otherwise(lit("n/a"))
            )
            .withColumn("cst_gndr", 
                when(upper(trim($"cst_gndr")) === "M", "Male")
                .when(upper(trim($"cst_gndr")) === "F", "Female")
                .otherwise(lit("n/a"))
            )
            .filter($"cst_id".isNotNull)
            .withColumn("flag_last", row_number().over(windowSpec))
            .filter($"flag_last" === 1)
            .withColumn("dwh_create_date", current_timestamp())
            .drop("flag_last", "ingestion_timestamp", "source")
        
        silverCustInfoDF.show(5)
        db.writeTable(silverCustInfoDF, "silver.crm_cust_info")

        println("Silver Customers Info transformation completed.\n")
    }

    def transformProductsInfo(): Unit = {

        println("Inserting Data Into: silver.crm_prd_info")

        val bronzePrdInfoDF = db.readTable("bronze.crm_prd_info")

        val windowSpec = Window.partitionBy("prd_key").orderBy($"prd_start_dt")
        
        val silverPrdInfoDF = bronzePrdInfoDF
            .withColumn("cat_id", regexp_replace(substring($"prd_key", 1, 5), "-", "_"))
            .withColumn("prd_key", substring($"prd_key", 7, 100))
            .withColumn("prd_cost", coalesce($"prd_cost", lit(0)))
            .withColumn("prd_line", 
                when(upper(trim($"prd_line")) === "M", "Mountain")
                .when(upper(trim($"prd_line")) === "R", "Road")
                .when(upper(trim($"prd_line")) === "T", "Touring")
                .when(upper(trim($"prd_line")) === "S", "Other Sales")
                .otherwise(lit("n/a"))
            )
            .withColumn("prd_start_dt", $"prd_start_dt".cast("date"))
            // Calculate end date as one day before the next start date
            .withColumn("prd_end_dt", date_sub(lead($"prd_start_dt", 1).over(windowSpec), 1))
            .withColumn("dwh_create_date", current_timestamp())
            .drop("ingestion_timestamp", "source")
        
        silverPrdInfoDF.show(5)
        db.writeTable(silverPrdInfoDF, "silver.crm_prd_info")
        
        println("Silver Products Info transformation completed.\n")
    }

    def transformSalesDetails(): Unit = {

        println("Inserting Data Into: silver.crm_sales_details")

        val bronzeSalesDetailsDF = db.readTable("bronze.crm_sales_details")
        
        val silverSalesDetailsDF = bronzeSalesDetailsDF
            .withColumn("sls_order_dt",
                when($"sls_order_dt" === 0 || length($"sls_order_dt".cast("string")) =!= 8, lit(null))
                .otherwise(to_date($"sls_order_dt".cast("string"), "yyyyMMdd"))
            )
            .withColumn("sls_ship_dt", 
                when($"sls_ship_dt" === 0 || length($"sls_ship_dt".cast("string")) =!=  8, lit(null))
                .otherwise(to_date($"sls_ship_dt".cast("string"), "yyyyMMdd"))
            )
            .withColumn("sls_due_dt", 
                when($"sls_due_dt" === 0 || length($"sls_due_dt".cast("string")) =!= 8, lit(null))
                .otherwise(to_date($"sls_due_dt".cast("string"), "yyyyMMdd"))
            )
            .withColumn("sls_sales", 
                when($"sls_sales".isNull || $"sls_sales" <= 0 
                    || $"sls_sales" =!= $"sls_quantity" * abs($"sls_price"), $"sls_quantity" * abs($"sls_price"))
                .otherwise($"sls_sales")
            )
            .withColumn("sls_price", 
                when($"sls_price".isNull || $"sls_price" <= 0,
                    $"sls_sales" / when($"sls_quantity" === 0, lit(null))
                    .otherwise($"sls_quantity")
                )
                .otherwise($"sls_price")
            )
            .filter($"sls_ord_num".isNotNull)
            .withColumn("dwh_create_date", current_timestamp())
            .drop("ingestion_timestamp", "source")        
        
        silverSalesDetailsDF.show(5)
        db.writeTable(silverSalesDetailsDF, "silver.crm_sales_details")
        
        println("Silver Sales Details transformation completed.\n")
    }

    def transformCustAdditionalInfo(): Unit = {

        println("Inserting Data Into: silver.erp_cust_az12")

        val bronzeCustAz12DF = db.readTable("bronze.erp_cust_az12")
        
        val silverCustAz12DF = bronzeCustAz12DF
            .withColumn("cid",
                when($"cid".like("NAS%"), substring($"cid", 4, 99))
                .otherwise($"cid")
            )
            .withColumn("bdate",
                when($"bdate" > current_date(), null)
                .otherwise($"bdate")
            )
            .withColumn("gen", 
                when(upper(trim($"gen")).isin("M", "MALE"), "Male")
                .when(upper(trim($"gen")).isin("F", "FEMALE"), "Female")
                .otherwise(lit("n/a"))
            )
            .withColumn("dwh_create_date", current_timestamp())
            .drop("ingestion_timestamp", "source")            
        
        silverCustAz12DF.show(5)
        db.writeTable(silverCustAz12DF, "silver.erp_cust_az12")
        
        println("Silver Customers Additionnal Info transformation completed.\n")
    }

    def transformLocAddress(): Unit = {

        println("Inserting Data Into: silver.erp_loc_a101")

        val bronzeLocA101DF = db.readTable("bronze.erp_loc_a101")

        import spark.implicits._
        val silverLocA101DF = bronzeLocA101DF
            .withColumn("cid", regexp_replace($"cid", "-", ""))
            .withColumn("cntry", 
                when(trim($"cntry") === "DE" , "Germany")
                .when(trim($"cntry").isin("US", "USA"), "United States")
                .when(trim($"cntry") === "" || $"cntry".isNull, "n/a")
                .otherwise(trim($"cntry"))
            )
            .withColumn("dwh_create_date", current_timestamp())
            .drop("ingestion_timestamp", "source")
        
        silverLocA101DF.show(5)
        db.writeTable(silverLocA101DF, "silver.erp_loc_a101")
        
        println("Silver Customer Country Location transformation completed.\n")
    }

    def transformProductsCategory(): Unit = {

        println("Inserting Data Into: silver.erp_px_cat_g1v2")

        val bronzePxCatG1V2DF = db.readTable("bronze.erp_px_cat_g1v2")
        
        val silverPxCatG1V2DF = bronzePxCatG1V2DF
            .withColumn("dwh_create_date", current_timestamp())
            .drop("ingestion_timestamp", "source")
        
        silverPxCatG1V2DF.show(5)
        db.writeTable(silverPxCatG1V2DF, "silver.erp_px_cat_g1v2")
        
        println("Silver Products Category transformation completed.\n")
    }

    def run(): Unit = {
        println("============================================================")
        println("============ Starting Silver Transformations ===============")
        println("============================================================")
        println()
        println("-----  Loading CRM Tables (Silver) -----")
        println()
        transformCustomersInfo()
        transformProductsInfo()
        transformSalesDetails()
        println("-----  Loading ERP Tables (Silver) -----")
        transformCustAdditionalInfo()
        transformLocAddress()
        transformProductsCategory()
        println()
        println("============================================================")
        println("=========== Silver Transformations completed ===============")
        println("============================================================")
    }
}



