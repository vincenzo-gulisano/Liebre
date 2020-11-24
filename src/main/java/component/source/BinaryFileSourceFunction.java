package component.source;

import common.util.Util;
import org.apache.commons.lang3.Validate;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.io.*;

public class BinaryFileSourceFunction<IN extends Serializable> implements SourceFunction<IN> {

  private static final Logger LOGGER = LogManager.getLogger();
  private static final long IDLE_SLEEP = 1000;
  private final String path;
  private ObjectInputStream reader;
  private volatile boolean done = false;
  private boolean enabled;

  public BinaryFileSourceFunction(String path) {
    Validate.notBlank(path, "path");
    this.path = path;
  }

  @Override
  public IN get() {
    if (done) {
      LOGGER.debug("Finished processing input. Sleeping...");
      Util.sleep(IDLE_SLEEP);
      return null;
    }
    return readNextLine();
  }

  private IN readNextLine() {
    IN result = null;
    try {
      result = (IN) reader.readObject();
    } catch (IOException e) {
      LOGGER.warn("Text Source failed to read", e);
    } catch (ClassNotFoundException e) {
      e.printStackTrace();
    }
    done = (result == null);
    return result;
  }

  @Override
  public boolean isInputFinished() {
    return done;
  }

  @Override
  public void enable() {
    try {
      this.reader = new ObjectInputStream(new FileInputStream(path));
    } catch (FileNotFoundException e) {
      throw new IllegalArgumentException(String.format("File not found: %s", path));
    } catch (IOException e) {
      e.printStackTrace();
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
