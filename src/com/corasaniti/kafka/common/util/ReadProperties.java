package com.corasaniti.kafka.common.util;

import java.io.FileInputStream;
import java.io.IOException;
import java.util.Properties;

public class ReadProperties {
	
	public static final String KAFKA_SERVERS = "kafka_servers";
	
	public static Properties  getProperties() throws IOException {
		FileInputStream fis = new FileInputStream("src/resources/config.properties");		
		Properties properties  = new Properties();
		properties.load(fis);				
		fis.close();
		return properties;
	}
	
	public static String getProperty(String key) throws IOException {
		Properties props = getProperties();
		return props.getProperty(key);
	}
}
