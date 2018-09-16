package com.config;

import java.sql.Driver;

import javax.sql.DataSource;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.autoconfigure.EnableAutoConfiguration;
import org.springframework.boot.autoconfigure.condition.ConditionalOnProperty;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.core.env.Environment;
import org.springframework.jdbc.datasource.SimpleDriverDataSource;

import lombok.extern.log4j.Log4j2;

@EnableAutoConfiguration
@Log4j2
@Configuration
public class DataBaseConfig {

	@Autowired 
	Environment env;
	
	@Bean
	@ConditionalOnProperty(prefix="spring.outputDataSource", name="url")
	public DataSource outputDataSource() throws Exception {
		return buildData("spring.outputDataSource");
	}

	private DataSource buildData(String prop) throws Exception{
		
		SimpleDriverDataSource ds= new SimpleDriverDataSource();
		
		String url=env.getProperty(prop+".url");
		String className =env.getProperty(prop+".driver.class");
		
		ds.setUrl(url);
		ds.setDriverClass((Class<Driver>) Class.forName(className));
		
		return ds;
	}	
}
