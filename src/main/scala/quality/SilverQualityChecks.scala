package quality

import org.apache.spark.sql.{DataFrame, SparkSession}
import org.apache.spark.sql.functions._
import config.AppConfig
import database.JdbcDataAccess



class SilverQualityChecks(spark: SparkSession, db: JdbcDataAccess) {

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
                s"Silver quality check failed: $checkName. Found $count invalid record(s)."
            )
        }

        println(s"PASSED: $checkName")
    }

    // ====================================================================
    // silver.crm_cust_info
    // ====================================================================
    private def checkCrmCustomerInfo(): Unit = {

        println()
        println("Checking silver.crm_cust_info...")

        val df = db.readTable("silver.crm_cust_info")

        // Check for NULLs or Duplicates in Primary Key
        val invalidKeys = df
            .groupBy("cst_id")
            .count()
            .filter(col("count") > 1 || col("cst_id").isNull)

        failIfRecordsFound(
            "Checking crm_cust_info NULL or duplicate cst_id", invalidKeys
        )

        // Check for unwanted spaces
        val unwantedSpaces = df
            .filter(col("cst_key") =!= trim(col("cst_key")))
            .select("cst_key")

        failIfRecordsFound(
            "Checking crm_cust_info unwanted spaces in cst_key", unwantedSpaces
        )

        // Data standardization
        println("Distinct cst_marital_status values:")
        df.select("cst_marital_status")
            .distinct()
            .show(false)
    }

    // ====================================================================
    // silver.crm_prd_info
    // ====================================================================
    private def checkCrmProductInfo(): Unit = {

        println()
        println("Checking silver.crm_prd_info...")

        val df = db.readTable("silver.crm_prd_info")

        // Check for NULLs or Duplicates in Primary Key
        val invalidKeys = df
            .groupBy("prd_id")
            .count()
            .filter(col("count") > 1 || col("prd_id").isNull)

        failIfRecordsFound(
            "Checking crm_prd_info NULL or duplicate prd_id", invalidKeys
        )

        // Check for unwanted spaces
        val unwantedSpaces = df
            .filter(col("prd_nm") =!= trim(col("prd_nm")))
            .select("prd_nm")

        failIfRecordsFound(
            "Checking crm_prd_info unwanted spaces in prd_nm", unwantedSpaces
        )

        // Check for NULL or negative cost
        val invalidCost = df
            .filter(col("prd_cost").isNull || col("prd_cost") < 0)
            .select("prd_cost")

        failIfRecordsFound(
            "Checking crm_prd_info  NULL or negative prd_cost", invalidCost
        )

        // Data standardization
        println("Distinct prd_line values:")
        df.select("prd_line")
            .distinct()
            .show(false)

        // Check invalid date orders
        val invalidDates = df
            .filter(col("prd_end_dt") < col("prd_start_dt"))

        failIfRecordsFound(
            "Checking crm_prd_info invalid date order", invalidDates
        )
    }

    // ====================================================================
    // silver.crm_sales_details
    // ====================================================================
    private def checkCrmSalesDetails(): Unit = {

        println()
        println("Checking silver.crm_sales_details...")

        val df = db.readTable("silver.crm_sales_details")

        // Check invalid date orders
        val invalidDateOrders = df
            .filter(
                col("sls_order_dt") > col("sls_ship_dt") ||
                col("sls_order_dt") > col("sls_due_dt")
            )

        failIfRecordsFound(
            "Checking crm_sales_details invalid order/shipping/due dates", invalidDateOrders
        )

        // Check Sales = Quantity * Price
        val inconsistentSales = df
            .filter(
                col("sls_sales") =!= col("sls_quantity") * col("sls_price") ||
                col("sls_sales").isNull ||
                col("sls_quantity").isNull ||
                col("sls_price").isNull ||
                col("sls_sales") <= 0 ||
                col("sls_quantity") <= 0 ||
                col("sls_price") <= 0
            )
            .select("sls_sales", "sls_quantity", "sls_price")
            .distinct()
            .orderBy("sls_sales", "sls_quantity", "sls_price")

        failIfRecordsFound(
            "Checking  crm_sales_details sales/quantity/price inconsistency", inconsistentSales
        )
    }

    // ====================================================================
    // silver.erp_cust_az12
    // ====================================================================
    private def checkErpCustomerAz12(): Unit = {

        println()
        println("Checking silver.erp_cust_az12...")

        val df = db.readTable("silver.erp_cust_az12")

        // Birthdate must be between 1924-01-01 and today
        val invalidBirthdates = df
            .filter(
                col("bdate") < lit("1924-01-01") ||
                col("bdate") > current_date()
            )
            .select("bdate")
            .distinct()

        failIfRecordsFound("Checking erp_cust_az12 invalid birthdate", invalidBirthdates)

        // Data standardization
        println("Distinct gen values:")
        df.select("gen")
            .distinct()
            .show(false)
    }

    // ====================================================================
    // silver.erp_loc_a101
    // ====================================================================
    private def checkErpLocationA101(): Unit = {

        println()
        println("Checking silver.erp_loc_a101...")

        val df = db.readTable("silver.erp_loc_a101")

        // Data standardization
        println("Distinct cntry values:")
        df.select("cntry")
            .distinct()
            .orderBy("cntry")
            .show(false)
    }

    // ====================================================================
    // silver.erp_px_cat_g1v2
    // ====================================================================
    private def checkErpProductCategory(): Unit = {

        println()
        println("Checking silver.erp_px_cat_g1v2...")

        val df = db.readTable("silver.erp_px_cat_g1v2")

        // Check for unwanted spaces
        val unwantedSpaces = df
            .filter(
                col("cat") =!= trim(col("cat")) ||
                col("subcat") =!= trim(col("subcat")) ||
                col("maintenance") =!= trim(col("maintenance"))
            )

        failIfRecordsFound("Checkign erp_px_cat_g1v2 unwanted spaces", unwantedSpaces)

        // Data standardization
        println("Distinct maintenance values:")
        df.select("maintenance")
            .distinct()
            .show(false)
    }

    def run(): Unit = {
        println()
        println("============================================================")
        println("============== Running Silver Quality Checks ===============")
        println("============================================================")
        println()
        checkCrmCustomerInfo()
        checkCrmProductInfo()
        checkCrmSalesDetails()
        checkErpCustomerAz12()
        checkErpLocationA101()
        checkErpProductCategory()
        println()
        println("============================================================")
        println("============= All Silver Quality Checks passed =============")
        println("============================================================")
    }
}




