package schema
import org.apache.spark.sql.types._


object SalesSchema {

    val schema: StructType = StructType(
        Seq(
            StructField("sls_ord_num", StringType, nullable = false),
            StructField("sls_prd_key", StringType, nullable = true),
            StructField("sls_cust_id", IntegerType, nullable = true),
            StructField("sls_order_dt", IntegerType, nullable = true),
            StructField("sls_ship_dt", IntegerType, nullable = true),
            StructField("sls_due_dt", IntegerType, nullable = true),
            StructField("sls_sales", IntegerType, nullable = true),
            StructField("sls_quantity", IntegerType, nullable = true),
            StructField("sls_price", IntegerType, nullable = true)
        )
    )
}




