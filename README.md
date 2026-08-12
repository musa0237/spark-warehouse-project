# Spark Warehouse Proeject (Scala)

========================


Compile:
    sbt clean assembly

Load .env variables in local environment variables:
    set -a
    source .env
    set +a

Run the jar:
    spark-submit --class WareHouseApp \
        target/scala-2.12/spark-warehouse-assembly-1.0-SNAPSHOT.jar





