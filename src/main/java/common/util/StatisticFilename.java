package common.util;

import java.io.File;

import common.NamedEntity;

public enum StatisticFilename {
	INSTANCE;
	public String get(String folder, NamedEntity entity, String type) {
		StringBuilder sb = new StringBuilder();
		sb.append(folder);
		sb.append(File.separator);
		sb.append(entity.getId());
		sb.append(".").append(type);
		sb.append(".csv");
		return sb.toString();
	}
}
