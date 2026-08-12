package schema
import org.apache.spark.sql.types._


object CustomerSchema {

    val schema: StructType = StructType(
        Seq(
            StructField("cst_id", IntegerType, nullable = false),
            StructField("cst_key", StringType, nullable = true),
            StructField("cst_firstname", StringType, nullable = true),
            StructField("cst_lastname", StringType, nullable = true),
            StructField("cst_marital_status", StringType, nullable = true),
            StructField("cst_gndr", StringType, nullable = true),
            StructField("cst_create_date", DateType, nullable = true)
        )
    )
}



