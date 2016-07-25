package com.bubblewrap.spark_cassandra_connector.test;

import com.bubblewrap.spark_cassandra_connector.SparkCassandraDriver;
import com.datastax.driver.core.ResultSet;
import com.datastax.driver.core.Row;

public class Test {

	public static void main(String[] args) {
		SparkCassandraDriver demo = new SparkCassandraDriver("Spark Cassandra Java API demo", "spark://localhost:7077",
				"localhost");

		ResultSet rs = demo.executeQuery("select * from test.table");

		for (Row row : rs.all()) {
			System.out.println(row.toString());
		}

		demo.stop();
	}

}
