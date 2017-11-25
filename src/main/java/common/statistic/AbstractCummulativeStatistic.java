package common.statistic;

import java.io.FileWriter;
import java.io.IOException;
import java.io.PrintWriter;
import java.util.concurrent.TimeUnit;

import common.Active;

public abstract class AbstractCummulativeStatistic<T extends Number> implements Active {

	private final PrintWriter out;

	public AbstractCummulativeStatistic(String outputFile, boolean autoFlush) {
		try {
			FileWriter outFile = new FileWriter(outputFile);
			out = new PrintWriter(outFile, autoFlush);
		} catch (IOException e) {
			throw new IllegalArgumentException(
					String.format("Failed to open file %s for writing: %s", outputFile, e.getMessage()), e);
		}
	}

	protected void writeCommaSeparatedValues(Object... values) {
		StringBuilder sb = new StringBuilder();
		for (Object value : values) {
			sb.append(value).append(",");
		}
		sb.deleteCharAt(sb.length() - 1);
		writeLine(sb.toString());
	}

	protected void writeLine(String line) {
		out.println(line);
	}

	protected long currentTimeSeconds() {
		return TimeUnit.MILLISECONDS.toSeconds(System.currentTimeMillis());
	}

	@Override
	public void disable() {
		out.close();
	}

	public abstract void append(T value);
}
