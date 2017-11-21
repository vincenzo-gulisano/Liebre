package common.util;

import java.io.IOException;
import java.io.InputStream;
import java.util.Properties;

public class PropertyLoader {

	private final Properties properties = new Properties();
	private static final String ERROR_TEMPLATE = "Failed to read properties from file: %s";

	public PropertyLoader(String filename, Class<?> clazz) {
		InputStream in = null;
		try {
			in = clazz.getClassLoader().getResourceAsStream(filename);
			if (in == null) {
				throw new IllegalArgumentException(String.format(ERROR_TEMPLATE, filename));
			}
			properties.load(in);
		} catch (IOException exception) {
			throw new IllegalArgumentException(String.format(ERROR_TEMPLATE, filename), exception);
		} finally {
			if (in != null) {
				try {
					in.close();
				} catch (IOException e) {
					e.printStackTrace();
				}
			}
		}
	}

	public String get(String key) {
		return properties.getProperty(key);
	}

}
