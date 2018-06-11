/*
 * Copyright (C) 2017-2018
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

package source;

import java.io.BufferedReader;
import java.io.FileNotFoundException;
import java.io.FileReader;
import java.io.IOException;

import common.tuple.Tuple;
import common.util.Util;

public class TextFileSource<T extends Tuple> extends AbstractSource<T> {

  private BufferedReader br;
  private String nextLine = "";
  private boolean hasNext = true;
  private final TextSourceFunction<T> function;

  public TextFileSource(String id, String filename, TextSourceFunction<T> function) {
    super(id);
    this.function = function;
    try {
      this.br = new BufferedReader(new FileReader(filename));
    } catch (FileNotFoundException e) {
      throw new IllegalArgumentException(String.format("File not found: %s", filename));
    }
  }

  //FIXME: Refactor
  public boolean hasNext() {
    if (!isEnabled()) {
      return false;
    }
    if (hasNext) {
      try {
        if ((nextLine = br.readLine()) == null) {
          br.close();
          hasNext = false;
        }
      } catch (IOException e) {
        e.printStackTrace();
      }
    } else {
      // Prevent spinning
      Util.sleep(1000);
    }
    return hasNext;
  }

  @Override
  public T getNextTuple() {
    if (hasNext()) {
      return function.getNext(nextLine);
    } else {
      return null;
    }
  }

  @Override
  public void disable() {
    if (hasNext) {
      try {
        br.close();
      } catch (IOException e) {
        e.printStackTrace();
      }
    }
  }

}
