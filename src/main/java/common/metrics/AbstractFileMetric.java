package common.metrics;

import java.io.File;
import java.io.FileWriter;
import java.io.IOException;
import java.io.PrintWriter;

public abstract class AbstractFileMetric extends AbstractMetric {

  private final PrintWriter out;

  public AbstractFileMetric(String id, String folder, boolean autoFlush) {
    super(id);
    final String outputFile = folder + File.separator + id + ".csv";
    try {
      FileWriter outFile = new FileWriter(outputFile);
      out = new PrintWriter(outFile, autoFlush);
    } catch (IOException e) {
      throw new IllegalArgumentException(
          String.format("Failed to open file %s for writing: %s", outputFile, e.getMessage()), e);
    }
  }

  protected final void writeCSVLine(Object... values) {
    StringBuilder sb = new StringBuilder();
    for (Object value : values) {
      sb.append(value).append(",");
    }
    sb.deleteCharAt(sb.length() - 1);
    writeLine(sb.toString());
  }

  protected final void writeLine(String line) {
    out.println(line);
  }

  @Override
  public void disable() {
    if (isEnabled()) {
      out.flush();
      out.close();
    }
    super.disable();
  }
}
