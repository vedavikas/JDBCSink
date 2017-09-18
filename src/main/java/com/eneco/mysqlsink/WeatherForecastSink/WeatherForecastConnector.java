package com.eneco.mysqlsink.WeatherForecastSink;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;

import org.apache.kafka.common.config.ConfigDef;
import org.apache.kafka.connect.connector.Task;
import org.apache.kafka.connect.sink.SinkConnector;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class WeatherForecastConnector extends SinkConnector {
	private static Logger log = LoggerFactory.getLogger(WeatherForecastConnector.class);
	private WeatherForecastConfig config;

	@Override
	public String version() {
		return VersionUtil.getVersion();
	}

	@Override
	public void start(Map<String, String> map) {
		log.info("Starting Connetor");
		config = new WeatherForecastConfig(map);
	}

	@Override
	public Class<? extends Task> taskClass() {
		return WeatherForecastTask.class;
	}

	@Override
	public List<Map<String, String>> taskConfigs(int maxTasks) {
		ArrayList<Map<String, String>> configs = new ArrayList<Map<String, String>>(1);
		configs.add(config.originalsStrings());
		return configs;
	}

	@Override
	public void stop() {
	}

	@Override
	public ConfigDef config() {
		return WeatherForecastConfig.conf();
	}
}
