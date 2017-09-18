package com.eneco.mysqlsink.WeatherForecastSink.util;

import java.sql.Connection;
import java.sql.DatabaseMetaData;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.ArrayList;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class MySQLJDBCCalls {
	private static Logger log = LoggerFactory.getLogger(MySQLJDBCCalls.class);
	private String url;
	private String username;
	private String password;
	private String driverName;
	Connection connection = null;

	public MySQLJDBCCalls(String myURL, String myUsername, String myPassword, String driverName) {
		this.url = myURL;
		this.username = myUsername;
		this.password = myPassword;
		this.driverName = driverName;
	}

	public void createConnection() throws ClassNotFoundException, SQLException	{
		try {
			Class.forName(driverName);
		} catch (ClassNotFoundException e) {
			log.error("JDBC Driver " + driverName + " not found !!");
			return;
		}

		log.info("MySQL JDBC Driver Registered!");

		try {
			connection = DriverManager
					.getConnection(url,username, password);

		} catch (SQLException e) {
			log.error("Connection Failed! Check stack trace");
			return;
		}

		if (connection != null) {
			log.info("Taking control of database now!");
		} else {
			log.error("Failed to make connection!");
		}
	}

	public void createTables() throws SQLException	{
		StringBuilder sqlKmni = new StringBuilder("create table kmni_data_source ");
		sqlKmni.append("(id int not null auto_increment, ");
		sqlKmni.append(" station_name varchar(255) not null, "); 
		sqlKmni.append(" lon double, "); 
		sqlKmni.append(" lat double, ");
		sqlKmni.append(" ta varchar(255),");
		sqlKmni.append(" ff varchar(255),");
		sqlKmni.append(" pp varchar(255),");
		sqlKmni.append(" dd varchar(255),");
		sqlKmni.append(" qg varchar(255),");
		sqlKmni.append(" datetimestamp varchar(255),");
		sqlKmni.append(" primary key (id));");
		System.out.println("sql statement " + sqlKmni.toString());
		
		StringBuilder sqlWeer = new StringBuilder("create table weer_data_source ");
		sqlWeer.append("(id int not null auto_increment, ");
		sqlWeer.append(" location varchar(255) not null, "); 
		sqlWeer.append(" lat double, "); 
		sqlWeer.append(" lon double, ");
		sqlWeer.append(" station_type varchar(255),");
		sqlWeer.append(" temperature double,");
		sqlWeer.append(" rainfall double,");
		sqlWeer.append(" wind_speed double,");
		sqlWeer.append(" wind_force double,");
		sqlWeer.append(" wind_direction varchar(255),");
		sqlWeer.append(" air_pressure double,");
		sqlWeer.append(" notes varchar(255),");
		sqlWeer.append(" datetime varchar(255),");
		sqlWeer.append(" primary key (id));");
		System.out.println("sql statement " + sqlWeer.toString());

		DatabaseMetaData md = connection.getMetaData();
		ResultSet rs = md.getTables(null, null, "%", null);
		int checkKMNIPresent = 0;
		int checkWEERPresent = 0;
		while (rs.next()) {
			log.trace("Table - " + rs.getString(3));
			if (rs.getString(3).equals("kmni_data_source"))	{
				checkKMNIPresent = 1;
				break;
			}
			
			if (rs.getString(3).equals("weer_data_source"))	{
				log.info("weer_data_source table already exists, proceed..");
				checkWEERPresent = 1;
				break;
			}
		}
		if (checkKMNIPresent == 0)	{
			log.info("Table KMNI_DATA_SOURCE not present, creating the table");
			connection.createStatement().executeUpdate(sqlKmni.toString());
		}
		
		if (checkWEERPresent == 0)	{
			log.info("Table WEER_DATA_SOURCE not present, creating the table");
			connection.createStatement().executeUpdate(sqlWeer.toString());
		}
	}

	public void insertIntoTable(String tableName, ArrayList<String> dataType, ArrayList<String> columnName, ArrayList<String> columnValue)	{

		StringBuilder insertStmt = new StringBuilder("INSERT INTO ");
		StringBuilder columnCount = new StringBuilder("(");
		insertStmt.append(tableName + " ( ");
		for	(int i=0;i<columnName.size();i++)	{
			if (i != (columnName.size() - 1))	{
				insertStmt.append(columnName.get(i) + ", ");
				columnCount.append("?,");
			}
			else	{
				insertStmt.append(columnName.get(i) + " ) VALUES ");
				columnCount.append("?)");
			}
		}
		String finalInsertStmt = insertStmt.append(columnCount).toString();
		System.out.println("Insert statement - " + finalInsertStmt);
		PreparedStatement preparedStatement = null;
		try {
			preparedStatement = connection.prepareStatement(finalInsertStmt);
			int count = 0;
			for (int index=0; index < columnName.size();index++){
				count = index + 1;
				if (dataType.get(index).equals("int")){
					preparedStatement.setInt(count, Integer.parseInt(columnValue.get(index)));
				}
				if (dataType.get(index).equals("String")){
					preparedStatement.setString(count, columnValue.get(index));
				}
				if (dataType.get(index).equals("float")){
					preparedStatement.setFloat(count, Float.parseFloat(columnValue.get(index)));
				}
				if (dataType.get(index).equals("double")){
					preparedStatement.setDouble(count, Double.parseDouble(columnValue.get(index)));
				}
				if (dataType.get(index).equals("long")){
					preparedStatement.setLong(count, Long.parseLong(columnValue.get(index)));
				}
			}
			preparedStatement.executeUpdate();
			log.info("Record inserted into table " + tableName );
		} catch (SQLException e) {
			log.error(e.getMessage());
			e.printStackTrace();
		} catch (Exception e)	{
			log.error(e.getMessage());
			e.printStackTrace();
		}
	}

	public void closeConnection() throws SQLException{
		log.info("Closing DB connectivity");
		connection.close();
	}
}
