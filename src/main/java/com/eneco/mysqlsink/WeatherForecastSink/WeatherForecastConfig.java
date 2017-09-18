package com.eneco.mysqlsink.WeatherForecastSink;

import org.apache.kafka.common.config.AbstractConfig;
import org.apache.kafka.common.config.ConfigDef;
import org.apache.kafka.common.config.ConfigDef.Type;
import org.apache.kafka.common.config.ConfigDef.Importance;

import java.util.Map;


public class WeatherForecastConfig extends AbstractConfig {

	public static final String TOPIC_CONFIG = "topics";
	private static final String TOPIC_DOC = "Topic to write data to ";
	public static final String DRIVER_CONFIG = "connector.url";
	private static final String DRIVER_DOC = "URL";
	public static final String USERNAME_CONFIG = "username";
	private static final String USERNAME_DOC = "Username";
	public static final String PASSWORD_CONFIG = "password";
	private static final String PASSWORD_DOC = "Password";
	public static final String DRIVERNAME_CONFIG = "driver.name";
	private static final String DRIVERNAME_DOC = "Driver Name";

	public WeatherForecastConfig(ConfigDef config, Map<String, String> parsedConfig) {
		super(config, parsedConfig);
	}

	public WeatherForecastConfig(Map<String, String> parsedConfig) {
		this(conf(), parsedConfig);
	}

	public static ConfigDef conf() {
		return new ConfigDef()
				.define(TOPIC_CONFIG, Type.STRING, Importance.HIGH, TOPIC_DOC)
				.define(DRIVER_CONFIG, Type.STRING, Importance.HIGH, DRIVER_DOC)
				.define(USERNAME_CONFIG, Type.STRING, Importance.HIGH, USERNAME_DOC)
				.define(PASSWORD_CONFIG, Type.STRING, Importance.HIGH, PASSWORD_DOC)
				.define(DRIVERNAME_CONFIG, Type.STRING, Importance.HIGH, DRIVERNAME_DOC);
	}

	public String getMyDriver(){
		return this.getString(DRIVERNAME_CONFIG);
	}

	public String getMyTopic(){
		return this.getString(TOPIC_CONFIG);
	}

	public String getMyURL(){
		return this.getString(DRIVER_CONFIG);
	}

	public String getMyUsername(){
		return this.getString(USERNAME_CONFIG);
	}
	public String getMyPassword(){
		return this.getString(PASSWORD_CONFIG);
	}
}
