package com.bubblewrap.spark_cassandra_connector;

import java.io.Serializable;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaSparkContext;

import com.datastax.driver.core.ResultSet;
import com.datastax.driver.core.Session;
import com.datastax.spark.connector.cql.CassandraConnector;

/**
 * 
 * @author Rahul
 *
 */
public class SparkCassandraDriver implements Serializable {

	private static final long serialVersionUID = -8771372966496589536L;
	private transient SparkConf conf;
	private CassandraConnector cassandraConnector;
	private JavaSparkContext sc;

	public SparkCassandraDriver(String appName, String sparkMasterUrl, String cassandraUrl) {
		this.conf = new SparkConf();
		conf.setAppName(appName);
		conf.setMaster(sparkMasterUrl);
		conf.set("spark.cassandra.connection.host", cassandraUrl);

		this.sc = new JavaSparkContext(conf);
		this.cassandraConnector = CassandraConnector.apply(sc.getConf());
	}

	/**
	 * Stops the open SparkContext
	 */
	public void stop() {
		this.sc.stop();
	}

	/**
	 * Executes the provided query using the spark-cassandra driver
	 * 
	 * @param query
	 * @return ResultSet
	 */
	public ResultSet executeQuery(String query) {
		try (Session session = cassandraConnector.openSession()) {
			return session.execute(query);
		}
	}

}