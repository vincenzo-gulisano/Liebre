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

/**
 * Base implementation of {@link Sink} that applies a given {@link SinkFunction} to each input
 * tuple.
 *
 * @param <IN> The type of input tuples.
 */
public class BaseSink<IN> extends AbstractSink<IN> {

  private final SinkFunction<IN> function;

  /**
   * Construct.
   *  @param id The unique ID of this component.
   * @param function The function to be applied to each input tuple.
   */
  public BaseSink(String id, SinkFunction<IN> function) {
    super(id);
    Validate.notNull(function, "function");
    this.function = function;
  }

  @Override
  public void processTuple(IN tuple) {
    if (function.isEnabled()) {
      function.accept(tuple);
    }
  }

  @Override
  public void enable() {
    function.enable();
    super.enable();
  }

  @Override
  public void disable() {
    System.out.println("Deactivating " + this.state.getId());
    super.disable();
    function.disable();
  }

  @Override
  public boolean canRun() {
    return function.canRun() && super.canRun();
  }
}
