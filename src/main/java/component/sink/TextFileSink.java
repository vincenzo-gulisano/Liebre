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

package component.sink;

import common.tuple.Tuple;
import java.io.FileWriter;
import java.io.IOException;
import java.io.PrintWriter;

/**
 * {@link Sink} implementation that writes values to text files. Every input tuple is mapped to a
 * line in the output file produced by a {@link TextSinkFunction}.
 *
 * @param <IN> The type of input tuples.
 * @see TextSinkFunction
 */
public class TextFileSink<IN extends Tuple> extends AbstractSink<IN> {

  private final TextSinkFunction<IN> function;
  private PrintWriter pw;

  /**
   * Construct.
   *
   * @param id The unique ID of this component.
   * @param filename The file path to write the data to.
   * @param function The {@link TextSinkFunction} that will map every input tuple to a string.
   */
  public TextFileSink(String id, String filename, TextSinkFunction<IN> function) {
    this(id, filename, function, true);
  }

  protected TextFileSink(String id, String filename, TextSinkFunction<IN> function,
      boolean autoFlush) {
    super(id);
    try {
      this.pw = new PrintWriter(new FileWriter(filename), autoFlush);
    } catch (IOException e) {
      throw new IllegalArgumentException(String.format("Cannot write to file :%s", filename));
    }
    this.function = function;
  }

  public final void processTuple(IN tuple) {
    if (!isEnabled()) {
      throw new IllegalStateException("Output stream is closed");
    }
    pw.println(function.apply(tuple));
  }

  @Override
  public void enable() {
    super.enable();
    function.enable();
  }

  public void disable() {
    super.disable();
    function.disable();
    pw.flush();
    pw.close();
  }

}
