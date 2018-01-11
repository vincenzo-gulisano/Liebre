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
		StringBuilder sb = new StringBuilder();
		sb.append(folder);
		sb.append(File.separator);
		sb.append(getPrefix(entity));
		sb.append(entity.getId());
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
