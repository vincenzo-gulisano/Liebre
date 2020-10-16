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

import org.apache.commons.lang3.Validate;

import java.io.FileOutputStream;
import java.io.IOException;
import java.io.ObjectOutputStream;
import java.io.Serializable;

/**
 * Function from tuples to strings.
 *
 * @param <IN> The type of input tuples.
 */
public class BinaryFileSinkFunction<IN extends Serializable> implements SinkFunction<IN> {

  private final String path;
  private boolean enabled;
  private ObjectOutputStream writer;

  public BinaryFileSinkFunction(String path) {
    Validate.notBlank(path, "path");
    this.path = path;
  }

  @Override
  public void accept(IN in) {
    try {
      writer.writeObject(in);
    } catch (IOException e) {
      e.printStackTrace();
    }
  }

  @Override
  public void enable() {
    try {
      this.writer = new ObjectOutputStream(new FileOutputStream(path));
    } catch (IOException e) {
      throw new IllegalArgumentException(String.format("Cannot write to file :%s", path));
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
      writer.writeObject(null);
      writer.close();
    } catch (IOException e) {
      e.printStackTrace();
    }
  }
}
