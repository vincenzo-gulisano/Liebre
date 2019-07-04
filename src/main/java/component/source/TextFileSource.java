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

package component.source;

import common.tuple.Tuple;
import common.util.Util;
import java.io.BufferedReader;
import java.io.FileNotFoundException;
import java.io.FileReader;
import java.io.IOException;
import org.apache.commons.lang3.Validate;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

/**
 * Source that reads lines from a text file and converts them to tuples.
 *
 * @param <OUT> The type of produced tuples.
 */
public class TextFileSource<OUT extends Tuple> extends AbstractSource<OUT> {

  private static final Logger LOGGER = LogManager.getLogger();
  private final TextSourceFunction<OUT> function;
  private BufferedReader br;
  private volatile boolean done = false;

  /**
   * Construct.
   *
   * @param id The unique ID of this component.
   * @param filename The file path where the data is read from.
   * @param function The {@link TextSourceFunction} that converts raw text lines to tuples.
   */
  public TextFileSource(String id,int relativeProducerIndex, String filename, TextSourceFunction<OUT> function) {
    super(id,relativeProducerIndex);
    Validate.notNull(function, "function");
    this.function = function;
    try {
      this.br = new BufferedReader(new FileReader(filename));
    } catch (FileNotFoundException e) {
      throw new IllegalArgumentException(String.format("File not found: %s", filename));
    }
  }

  @Override
  public OUT getNextTuple() {
    if (!done) {
      return function.apply(nextLine());
    }
    LOGGER.debug("Text Source {} has finished processing input. Sleeping...", getId());
    // If done, prevent spinning
    Util.sleep(1000);
    return null;
  }

  @Override
  public boolean canRun() {
    return !done && super.canRun();
  }

  @Override
  public boolean isEnabled() {
    return !done && super.isEnabled();
  }

  private String nextLine() {
    String nextLine = null;
    try {
      nextLine = br.readLine();
    } catch (IOException e) {
      LOGGER.warn("Text Source failed to read", e);
    }
    done = (nextLine == null);
    return nextLine;
  }

  @Override
  public void enable() {
    super.enable();
    function.enable();
  }

  @Override
  public void disable() {
    function.disable();
    super.disable();
    try {
      br.close();
    } catch (IOException e) {
      LOGGER.warn("Text Source failed to close file", e);
    }
  }
}

