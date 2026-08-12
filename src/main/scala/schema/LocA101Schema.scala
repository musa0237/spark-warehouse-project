package schema
import org.apache.spark.sql.types._


object LocA101Schema {

    val schema: StructType = StructType(
        Seq(
            StructField("cid", StringType, nullable = false),
            StructField("cntry", StringType, nullable = true)
        )
    )
}


