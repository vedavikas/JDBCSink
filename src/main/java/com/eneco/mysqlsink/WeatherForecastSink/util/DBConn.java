package com.eneco.mysqlsink.WeatherForecastSink.util;

import java.sql.Connection;
import java.sql.DatabaseMetaData;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;

public class DBConn {

	public static void main(String[] argv) throws SQLException {

		System.out.println("-------- MySQL JDBC Connection Testing ------------");
		try {
			Class.forName("com.mysql.jdbc.Driver");
		} catch (ClassNotFoundException e) {
			System.out.println("MySQL JDBC Driver not found !!");
			e.printStackTrace();
			return;
		}

		System.out.println("MySQL JDBC Driver Registered!");
		Connection connection = null;

		try {
			connection = DriverManager
					.getConnection("jdbc:mysql://localhost:3306/WEAHTERFORECAST?useLegacyDatetimeCode=false&serverTimezone=America/New_York","weather", "weather");

		} catch (SQLException e) {
			System.out.println("Connection Failed! Check output console");
			e.printStackTrace();
			return;
		}

		if (connection != null) {
			System.out.println("You made it, take control your database now!");
		} else {
			System.out.println("Failed to make connection!");
		}

		StringBuilder sql = new StringBuilder("CREATE TABLE TEST ");
		sql.append("(USER_ID INTEGER not NULL, ");
		sql.append(" USERNAME VARCHAR(255), "); 
		sql.append(" CREATED_BY VARCHAR(255), "); 
		sql.append(" CREATED_DATE DATE, ");
		sql.append(" PRIMARY KEY ( USER_ID ))");
		System.out.println("sql statement " + sql.toString());

		DatabaseMetaData md = connection.getMetaData();
		ResultSet rs = md.getTables(null, null, "%", null);
		while (rs.next()) {
			System.out.println(rs.getString(3));
			if (rs.getString(3).equals("TEST"))	{
				System.out.println("Registration table already exists");
				break;
			}
		}
		connection.createStatement().executeUpdate(sql.toString());

		String insertTableSQL = "INSERT INTO TEST"
				+ "(USER_ID, USERNAME, CREATED_BY, CREATED_DATE) VALUES"
				+ "(?,?,?,?)";
		PreparedStatement preparedStatement = null;
		try {
			preparedStatement = connection.prepareStatement(insertTableSQL);

			preparedStatement.setInt(1, 11);
			preparedStatement.setString(2, "mkyong");
			preparedStatement.setString(3, "system");
			preparedStatement.setTimestamp(4, getCurrentTimeStamp());
			// execute insert SQL stetement
			preparedStatement.executeUpdate();
			System.out.println("Record is inserted into DBUSER table!");
		} catch (SQLException e) {
			System.out.println(e.getMessage());
		} finally {

			if (preparedStatement != null) {
				preparedStatement.close();
			}
			if (connection != null) {
				connection.close();
			}
		}
	}
	private static java.sql.Timestamp getCurrentTimeStamp() {

		java.util.Date today = new java.util.Date();
		return new java.sql.Timestamp(today.getTime());

	}
}
