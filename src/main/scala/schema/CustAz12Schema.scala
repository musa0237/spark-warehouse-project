package schema
import org.apache.spark.sql.types._


object CustAz12Schema {

    val schema: StructType = StructType(
        Seq(
            StructField("cid", StringType, nullable = false),
            StructField("bdate", DateType, nullable = true),
            StructField("gen", StringType, nullable = true)
        )
    )
}



