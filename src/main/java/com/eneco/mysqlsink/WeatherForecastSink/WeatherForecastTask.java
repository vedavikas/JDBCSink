package com.eneco.mysqlsink.WeatherForecastSink;

import java.sql.SQLException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.HashSet;
import java.util.Iterator;
import java.util.Map;
import java.util.Set;

import org.apache.kafka.clients.consumer.OffsetAndMetadata;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.connect.sink.SinkRecord;
import org.apache.kafka.connect.sink.SinkTask;
import org.json.simple.JSONObject;
import org.json.simple.parser.JSONParser;
import org.json.simple.parser.ParseException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import com.eneco.mysqlsink.WeatherForecastSink.util.MySQLJDBCCalls;

public class WeatherForecastTask extends SinkTask {
	private static Logger log = LoggerFactory.getLogger(WeatherForecastTask.class);
	public MySQLJDBCCalls calls;
	public WeatherForecastConfig config;
	public ArrayList<String> dataType = new ArrayList<String>();
	public ArrayList<String> columnName = new ArrayList<String>();
	public ArrayList<String> columnValue = new ArrayList<String>();
	public String tableName;
	@Override
	public String version() {
		return VersionUtil.getVersion();
	}

	@Override
	public void start(Map<String, String> map) {
		log.info("Starting task");
		config = new WeatherForecastConfig(map);
		log.info("Establishing db connection");
		calls = new MySQLJDBCCalls(config.getMyURL(), config.getMyUsername(), config.getMyPassword(),config.getMyDriver());
		try {
			calls.createConnection();
			log.info("MYSQL DB connection established!!");
		} catch (ClassNotFoundException e) {
			e.printStackTrace();
		} catch (SQLException e) {
			e.printStackTrace();
		}

		try {
			calls.createTables();
		} catch (SQLException e) {
			log.error("SQLException encountered");
			e.printStackTrace();
		}
	}

	@SuppressWarnings("unchecked")
	@Override
	public void put(Collection<SinkRecord> collection) {
		if (collection.isEmpty()) {
			return;
		}	
		JSONParser parser = new JSONParser();
		for (SinkRecord record : collection)	{
			dataType.clear();
			columnName.clear();
			columnValue.clear();
			log.info("Value of record " + record.topic() + " --> " + record.value());
			try {
				if (record.topic().equals("knmids_topic"))	{
					tableName = "kmni_data_source";
				}
				else if (record.topic().equals("weeractualds_topic"))	{
					tableName = "weer_data_source";
				}
				Object obj = parser.parse((String) record.value());
				JSONObject jsonObject = (JSONObject) obj;
				Set<String> set = new HashSet<String>();

				set = jsonObject.keySet();
				Iterator<String> iter = set.iterator();
				Object values;
				String key;
				while (iter.hasNext()) {
					key = iter.next();
					values = jsonObject.get(key);
					log.trace("Key - " + key);
					log.trace("Value - " + values);
					if(values instanceof Integer){
						log.trace(values + " is of type Integer");
						dataType.add("int");
						columnName.add(key);
						columnValue.add(String.valueOf(values));

					}
					else if (values instanceof String)	{
						log.trace(values + " is of type String");
						dataType.add("String");
						columnName.add(key);
						columnValue.add(String.valueOf(values));
					}
					else if (values instanceof Double)	{
						log.trace(values + " is of type Double");
						dataType.add("double");
						columnName.add(key);
						columnValue.add(String.valueOf(values));
					}
					else if (values instanceof Long)	{
						log.trace(values + " is of type Long");
						dataType.add("long");
						columnName.add(key);
						columnValue.add(String.valueOf(values));
					}
					else if (values instanceof Float)	{
						log.trace(values + " is of type Float");
						dataType.add("float");
						columnName.add(key);
						columnValue.add(String.valueOf(values));
					}
				}
				calls.insertIntoTable(tableName, dataType, columnName, columnValue);			
			} catch (ParseException e) {
				log.error("Error Parsing JSON on topic ");
				e.printStackTrace();
			}
		} 

	}

	@Override
	public void flush(Map<TopicPartition, OffsetAndMetadata> map) {

	}

	@Override
	public void stop() {
		log.info("Stopping task");
		log.info("Closing DB connectivity");;
		try {
			calls.closeConnection();
		} catch (SQLException e) {
			log.error("Facing Trouble in closing DB connection, please check below stacktrace:");
			e.printStackTrace();
		}
	}

}
