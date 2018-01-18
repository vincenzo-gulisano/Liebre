package common.util;

import java.io.File;

import common.NamedEntity;
import operator.Operator;
import sink.Sink;
import source.Source;
import stream.Stream;

public enum StatisticFilename {
	INSTANCE;
	public String get(String folder, NamedEntity entity, String type) {
		String name = getPrefix(entity) + entity.getId();
		return get(folder, name, type);
	}

	public String get(String folder, String filename, String type) {
		StringBuilder sb = new StringBuilder();
		sb.append(folder);
		sb.append(File.separator);
		sb.append(filename);
		sb.append(".").append(type);
		sb.append(".csv");
		return sb.toString();
	}

	private String getPrefix(NamedEntity entity) {
		if (entity instanceof Operator) {
			return "OPERATOR_";
		} else if (entity instanceof Source) {
			return "SOURCE_";
		} else if (entity instanceof Sink) {
			return "SINK_";
		} else if (entity instanceof Stream) {
			return "STREAM_";
		} else {
			return "";
		}
	}
}
