package database
import java.sql.DriverManager
import config.AppConfig


object DatabaseInitializer {

    def initialize(): Unit = {

        Class.forName(AppConfig.databaseDriver) 
        val connection = DriverManager.getConnection(
            AppConfig.jdbcUrl, 
            AppConfig.databaseUser, 
            AppConfig.databasePassword 
        )

        try {
            val statement = connection.createStatement()

            statement.execute("CREATE SCHEMA IF NOT EXISTS bronze")
            statement.execute("CREATE SCHEMA IF NOT EXISTS silver")
            statement.execute("CREATE SCHEMA IF NOT EXISTS gold")

            statement.close()
            println("PostgreSQL schemas initialized successfully.")
        } 
        finally {
            connection.close()
        }
    }
}



