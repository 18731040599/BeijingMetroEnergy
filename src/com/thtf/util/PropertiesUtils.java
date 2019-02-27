package com.thtf.util;

import java.io.IOException;
import java.io.InputStream;
import java.util.Properties;

public class PropertiesUtils {
	private static Properties baseProps;

	public static String getDatabasePropertiesByKey(String key) {
		return baseProps.getProperty(key);
	}

	static {
		if (baseProps == null) {
			baseProps = new Properties();
			InputStream in = PropertiesUtils.class.getResourceAsStream("params.properties");
			try {
				baseProps.load(in);
			} catch (IOException e1) {
				// TODO Auto-generated catch block
				e1.printStackTrace();
				System.out.println("Read config exception !");
			}
		}
	}
}
