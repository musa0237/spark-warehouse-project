package config
import com.typesafe.config.ConfigFactory



object AppConfig {

	private val config = ConfigFactory.load()

	// Application
	val applicationName: String = config.getString("app.name")

	// Spark
	val sparkMaster: String = config.getString("spark.master")

	// Database
	val databaseHost: String = config.getString("database.host")
	val databasePort: Int = config.getInt("database.port")
	val databaseName: String = config.getString("database.name")
	val databaseUser: String = config.getString("database.user")
	val databasePassword: String = config.getString("database.password")
	val databaseDriver: String = config.getString("database.driver")

	// Paths
	val custInfoPath: String = config.getString("paths.cust_info")
	val prodInfoPath: String = config.getString("paths.prod_info")
	val salesDetailsPath: String = config.getString("paths.sales_details")
	val locA101Path: String = config.getString("paths.loc_a101")
	val custAz12Path: String = config.getString("paths.cust_az12")
	val pxCatG1v2Path: String = config.getString("paths.px_cat_g1v2")

	// JDBC URL
	val jdbcUrl: String = s"jdbc:postgresql://$databaseHost:$databasePort/$databaseName"

	def jdbcProperties(): java.util.Properties = {

		val properties = new java.util.Properties()

		properties.setProperty("user", databaseUser)
		properties.setProperty("password", databasePassword)
		properties.setProperty("driver", databaseDriver)

		properties
	}
}





