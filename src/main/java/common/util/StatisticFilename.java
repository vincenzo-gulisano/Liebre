/*
 * Copyright (C) 2017-2019
 *   Vincenzo Gulisano
 *   Dimitris Palyvos-Giannas
 *
 * This program is free software: you can redistribute it and/or modify
 * it under the terms of the GNU General Public License as published by
 * the Free Software Foundation, either version 3 of the License, or
 * (at your option) any later version.
 *
 * This program is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU General Public License for more details.
 *
 * You should have received a copy of the GNU General Public License
 * along with this program.  If not, see <http://www.gnu.org/licenses/>.
 *
 * Contact:
 *   Vincenzo Gulisano info@vincenzogulisano.com
 *   Dimitris Palyvos-Giannas palyvos@chalmers.se
 */

package common.util;

import java.io.File;

import common.Named;
import operator.Operator;
import scheduling.thread.LiebreThread;
import sink.Sink;
import sink.SinkFunction;
import source.Source;
import source.SourceFunction;
import stream.Stream;

/**
 * Helper class that forces a specific structure for the names of the files used by various
 * entities.
 * FIXME: Refactor, more intuitive strucutre.
 */
public enum StatisticFilename {
	INSTANCE;
	public String get(String folder, Named entity, String type) {
		String name = entity.getId();
		return get(folder, name, type);
	}

	public String get(String folder, LiebreThread thread, String type) {
		String name = String.valueOf(thread.getId());
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

	private String getPrefix(Object entity) {
		if (entity instanceof Operator) {
			return "OPERATOR_";
		} else if (entity instanceof Source || entity instanceof SourceFunction) {
			return "SOURCE_";
		} else if (entity instanceof Sink || entity instanceof SinkFunction) {
			return "SINK_";
		} else if (entity instanceof Stream) {
			return "STREAM_";
		} else if (entity instanceof Thread) {
			return "THREAD_";
		} else {
			return "";
		}
	}
}
