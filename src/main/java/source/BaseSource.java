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

import common.tuple.Tuple;
import org.apache.commons.lang3.Validate;

public class BaseSource<OUT extends Tuple> extends AbstractSource<OUT> {

  private final SourceFunction<OUT> function;

  public BaseSource(String id, SourceFunction<OUT> function) {
    super(id);
    Validate.notNull(function, "function");
    this.function = function;
  }

  @Override
  public OUT getNextTuple() {
    return function.getNextTuple();
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
  }

}
