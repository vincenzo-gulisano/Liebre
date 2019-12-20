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

import org.apache.commons.lang3.Validate;

/**
 * Base implementation of {@link Source} that generates data through a {@link SourceFunction}l
 *
 * @param <OUT> The type of output tuples.
 */
public class BaseSource<OUT> extends AbstractSource<OUT> {

  private final SourceFunction<OUT> function;

  /**
   * Construct.
   *  @param id The unique ID of this component.
   * @param function The {@link SourceFunction} that generates the output tuples.
   */
  public BaseSource(String id, SourceFunction<OUT> function) {
    super(id);
    Validate.notNull(function, "function");
    this.function = function;
  }

  @Override
  public double getHeadArrivalTime() {
    return function.getHeadArrivalTime();
  }

  @Override
  public double getAverageArrivalTime() {
    return function.getAverageArrivalTime();
  }

  @Override
  public OUT getNextTuple() {
    return function.get();
  }

  @Override
  public void enable() {
    function.enable();
    super.enable();
  }

  @Override
  public void disable() {
    super.disable();
    function.disable();
  }

  @Override
  public boolean canRun() {
    return function.canRun() && super.canRun();
  }
}
