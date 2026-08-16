package database

import org.apache.spark.sql.{DataFrame, SparkSession}
import config.AppConfig



class JdbcDataAccess(spark: SparkSession) {

    def readTable(tableName: String): DataFrame = {
        spark.read
            .jdbc(
                AppConfig.jdbcUrl, tableName, AppConfig.jdbcProperties()
            )
    }

    def writeTable(df: DataFrame, tableName: String): Unit = {
        df.write
            .mode("overwrite")
            .jdbc(
                AppConfig.jdbcUrl, tableName, AppConfig.jdbcProperties()
            )
    }
}




