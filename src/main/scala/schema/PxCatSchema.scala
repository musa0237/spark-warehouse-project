package schema
import org.apache.spark.sql.types._


object PxCatSchema {

    val schema: StructType = StructType(
        Seq(
            StructField("id", StringType, nullable = false),
            StructField("cat", StringType, nullable = true),
            StructField("subcat", StringType, nullable = true),
            StructField("maintenance", StringType, nullable = true)
        )
    )
}



