/*  Copyright (C) 2017  Vincenzo Gulisano
 *
 *  This program is free software: you can redistribute it and/or modify
 *  it under the terms of the GNU General Public License as published by
 *  the Free Software Foundation, either version 3 of the License, or
 *  (at your option) any later version.
 *
 *  This program is distributed in the hope that it will be useful,
 *  but WITHOUT ANY WARRANTY; without even the implied warranty of
 *  MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 *  GNU General Public License for more details.
 *
 *  You should have received a copy of the GNU General Public License
 *  along with this program.  If not, see <http://www.gnu.org/licenses/>.
 *
 *  Contact: Vincenzo Gulisano info@vincenzogulisano.com
 *
 */

package sink;

import common.tuple.Tuple;
import java.io.FileWriter;
import java.io.IOException;
import java.io.PrintWriter;

public class TextFileSink<T extends Tuple> extends AbstractSink<T> {

  private final TextSinkFunction<T> function;
  private PrintWriter pw;

  public TextFileSink(String id, String filename, TextSinkFunction<T> function) {
    this(id, filename, function, true);
  }

  protected TextFileSink(String id, String filename, TextSinkFunction<T> function,
      boolean autoFlush) {
    super(id);
    try {
      this.pw = new PrintWriter(new FileWriter(filename), autoFlush);
    } catch (IOException e) {
      throw new IllegalArgumentException(String.format("Cannot write to file :%s", filename));
    }
    this.function = function;
  }

  public final void processTuple(T tuple) {
    if (!isEnabled()) {
      throw new IllegalStateException("Output stream is closed");
    }
    pw.println(function.processTuple(tuple));
  }

  public void disable() {
    super.disable();
    pw.flush();
    pw.close();
  }

}
