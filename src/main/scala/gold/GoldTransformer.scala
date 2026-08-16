package gold
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._
import org.apache.spark.sql.expressions.Window
import config.AppConfig
import database.JdbcDataAccess


class GoldTransformer(spark: SparkSession, db: JdbcDataAccess) {

    import spark.implicits._

    private def createGoldDimCustomers(): Unit = {

        println("Create Dimension: gold.dim_customers")

        val silverCustInfoDF = db.readTable("silver.crm_cust_info")
        val silverCustAz12DF = db.readTable("silver.erp_cust_az12")
        val silverLocA101DF = db.readTable("silver.erp_loc_a101")

        val windowSpec = Window.orderBy($"cst_id")

        val joinedDF = silverCustInfoDF
            .join(silverCustAz12DF.as("ca"), $"ca.cid" === $"cst_key", "left")
            .join(silverLocA101DF.as("la"), $"la.cid" === $"cst_key", "left")
            .withColumn("gender",
                when($"cst_gndr" =!= "n/a", $"cst_gndr")
                .otherwise(coalesce($"ca.gen", lit("n/a")))
            )
            .withColumn("customer_key", row_number().over(windowSpec)) // Surrogate key
            .select(
                $"customer_key",
                $"cst_id".as("customer_id"),
                $"cst_key".as("customer_number"),
                $"cst_firstname".as("first_name"),
                $"cst_lastname".as("last_name"),
                $"cntry".as("country"),
                $"cst_marital_status".as("marital_status"),
                 $"gender",
                $"bdate".as("birthdate"),
                $"cst_create_date".as("create_date")
            )

        joinedDF.show(5)
        db.writeTable(joinedDF, "gold.dim_customers")

        println("Gold Dimension gold.dim_customers completed.\n")
    }

    private def createGoldDimProducts(): Unit = {

        println("Create Dimension: gold.dim_products")

        val silverPrdInfoDF = db.readTable("silver.crm_prd_info")
        val silverPxCatG1V2DF = db.readTable("silver.erp_px_cat_g1v2")
        
        val windowSpec = Window.orderBy($"pd.prd_start_dt", $"pd.prd_key")

        val joinedDF = silverPrdInfoDF.as("pd")
            .join(silverPxCatG1V2DF.as("pc"), $"pd.cat_id" === $"pc.id", "left")
            .filter($"pd.prd_end_dt".isNull) // Filter out all historical data
            .withColumn("product_key", row_number().over(windowSpec)) // Surrogate key
            .select(
                $"product_key",
                $"pd.prd_id".as("product_id"),
                $"pd.prd_key".as("product_number"),
                $"pd.prd_nm".as("product_name"),
                $"pd.cat_id".as("category_id"),
                $"pc.cat".as("category"),
                $"pc.subcat".as("subcategory"),
                $"pc.maintenance".as("maintenance"),
                $"pd.prd_cost".as("cost"),
                $"pd.prd_line".as("product_line"),
                $"pd.prd_start_dt".as("start_date")
            )
            
        joinedDF.show(5)
        joinedDF.printSchema
        db.writeTable(joinedDF, "gold.dim_products")

        println("Gold Dimension gold.dim_products completed.\n")
    }

    private def createGoldFactSales(): Unit = {

        println("Create Fact: gold.fact_sales")

        val silverSalesDetailsDF = db.readTable("silver.crm_sales_details")
        val dimCustomersDF = db.readTable("gold.dim_customers")
        val dimProductsDF = db.readTable("gold.dim_products")

        val joinedDF = silverSalesDetailsDF.as("sd")
            .join(dimProductsDF.as("pr"), $"sd.sls_prd_key" === $"pr.product_number", "left")
            .join(dimCustomersDF.as("cu"), $"sd.sls_cust_id" === $"cu.customer_id", "left")
            .select(
                $"sd.sls_ord_num".as("order_number"),
                $"pr.product_key".as("product_key"),
                $"cu.customer_key".as("customer_key"),
                $"sd.sls_order_dt".as("order_date"),
                $"sd.sls_ship_dt".as("shipping_date"),
                $"sd.sls_due_dt".as("due_date"),
                $"sd.sls_sales".as("sales_amount"),
                $"sd.sls_quantity".as("quantity"),
                $"sd.sls_price".as("price")
            )

        joinedDF.printSchema
        db.writeTable(joinedDF, "gold.fact_sales")

        println("Gold Fact gold.fact_sales completed.\n")
    }

    def run(): Unit = {
        println("============================================================")
        println("============== Starting Gold Transformations ===============")
        println("============================================================")
        println()
        createGoldDimCustomers()
        createGoldDimProducts()
        createGoldFactSales()
        println()
        println("============================================================")
        println("============= Gold Transformations completed ===============")
        println("============================================================")
    }
}



