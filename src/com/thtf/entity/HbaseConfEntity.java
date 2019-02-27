package com.thtf.entity;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.mapreduce.TableInputFormat;

public class HbaseConfEntity {
	private static Configuration configuration = null;

	private HbaseConfEntity() {
	}

	public static Configuration getHbaseConf(String tablename) {
		if (configuration == null) {
			configuration = HBaseConfiguration.create();
			configuration.set("hbase.zookeeper.property.clientPort", "2181");
			configuration.set("hbase.zookeeper.quorum", "stest");
		}
		configuration.set(TableInputFormat.INPUT_TABLE, tablename);
		return configuration;
	}
}
