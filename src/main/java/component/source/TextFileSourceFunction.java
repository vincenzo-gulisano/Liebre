package component.source;

import common.util.Util;
import java.io.BufferedReader;
import java.io.FileNotFoundException;
import java.io.FileReader;
import java.io.IOException;
import org.apache.commons.lang3.Validate;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

public class TextFileSourceFunction implements SourceFunction<String> {

  private static final Logger LOGGER = LogManager.getLogger();
  private static final long IDLE_SLEEP = 1000;
  private final String path;
  private BufferedReader reader;
  private volatile boolean done = false;
  private boolean enabled;

  public TextFileSourceFunction(String path) {
    Validate.notBlank(path, "path");
    this.path = path;
  }

  @Override
  public String get() {
    if (done) {
      LOGGER.debug("Finished processing input. Sleeping...");
      Util.sleep(IDLE_SLEEP);
      return null;
    }
    return readNextLine();
  }

  private String readNextLine() {
    String nextLine = null;
    try {
      nextLine = reader.readLine();
    } catch (IOException e) {
      LOGGER.warn("Text Source failed to read", e);
    }
    done = (nextLine == null);
    return nextLine;
  }

  @Override
  public void enable() {
    try {
      this.reader = new BufferedReader(new FileReader(path));
    } catch (FileNotFoundException e) {
      throw new IllegalArgumentException(String.format("File not found: %s", path));
    }
    this.enabled = true;
  }

  @Override
  public boolean isEnabled() {
    return enabled;
  }

  @Override
  public void disable() {
    this.enabled = false;
    try {
      this.reader.close();
    } catch (IOException e) {
      LOGGER.warn("Problem closing file {}: {}", path, e);
    }
  }

  @Override
  public boolean canRun() {
    return !done;
  }
}
