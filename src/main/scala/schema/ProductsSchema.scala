package schema
import org.apache.spark.sql.types._


object ProductSchema {

    val schema: StructType = StructType(
        Seq(
            StructField("prd_id", IntegerType, nullable = false),
            StructField("prd_key", StringType, nullable = true),
            StructField("prd_nm", StringType, nullable = true),
            StructField("prd_cost", IntegerType, nullable = true),
            StructField("prd_line", StringType, nullable = true),
            StructField("prd_start_dt", DateType, nullable = true),
            StructField("prd_end_dt", DateType, nullable = true)
        )
    )
}


