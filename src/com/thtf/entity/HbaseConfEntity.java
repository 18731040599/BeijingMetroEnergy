package com.thtf.entity;

import java.io.File;
import java.io.FileInputStream;
import java.util.Properties;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.mapreduce.TableInputFormat;

public class HbaseConfEntity {
	private static Configuration configuration = null;

	private HbaseConfEntity() {
	}

	public static Configuration getHbaseConf() {
		if (configuration == null) {
			Properties settings = new Properties();
				    try {
				    	settings.load(new FileInputStream(new File("settings.properties")));
				    }catch (Exception e) {
						// TODO: handle exception
				    	e.printStackTrace();
				    	System.out.println("Error reading default configuration file!");
					}
			configuration = HBaseConfiguration.create();
			configuration.set("hbase.zookeeper.property.clientPort", settings.getProperty("hbase.zookeeper.property.clientPort"));
			configuration.set("hbase.zookeeper.quorum", settings.getProperty("hbase.zookeeper.quorum"));
		}
		return configuration;
	}
}
