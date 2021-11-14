package cs523.SparkSqlHive;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;


public class App 
{
    public static void main( String[] args )
    {
    	
    	SparkConf sparkConf = new SparkConf();
		sparkConf.setAppName("Spark Sql Hive");
		sparkConf.setMaster("local");
		
	    sparkConf.set("hive.metastore.uris", "thrift://localhost:9083"); //9083, 2181
    	
	    SparkSession sparkSession = SparkSession.builder().appName("Spark SQL-Hive").config(sparkConf)
	    		 .config("hive.metastore.warehouse.external.dir", "/user/cloudera/HiveInput2")
	    		.config("spark.sql.hive.hiveserver2.jdbc.url", "jdbc:hive2://localhost:10000")
	    		  .enableHiveSupport().getOrCreate();
    	
	    sparkSession.catalog().listDatabases().show();
	    
	   // sparkSession.sqlContext().sql("use database_prog");
	   // System.out.println( "set Database!");
    	//Dataset<Row> tabledata = sparkSession.sql("SELECT * FROM employee");
        //tabledata.show();
//        sparkSession.sql("CREATE EXTERNAL TABLE IF NOT EXISTS other_employees (eid int, name String, "
//        	+" salary String, designation String)"
//        	+" COMMENT 'Employee details'"
//        	+" ROW FORMAT DELIMITED"
//        	+" FIELDS TERMINATED BY '\t'"
//        	+" LINES TERMINATED BY '\n'" 
//        	+" LOCATION 'hdfs:///user/cloudera/HiveInput2'");
        
       // sparkSession.sql("LOAD DATA INPATH '/user/cloudera/HiveInput2/moreEmps.txt' INTO TABLE employee");
        
        Dataset<Row>  tabledata = sparkSession.sql("SELECT * FROM other_employees");
        tabledata.show();
    	

    }
}
