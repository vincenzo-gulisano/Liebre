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

package component;

/**
 * Encapsulation of the basic execution logic for all {@link Component}s. This is required in order
 * to have reusable decorators without the need to duplicate code (i.e. the
 * process() function) and without having to resort to method interceptors.
 * <br/>
 *
 * @param <T> The component.operator subclass used.
 * @author palivosd
 */
public abstract class AbstractProcessCommand<T extends Component> implements ProcessCommand {

  protected final T component;

  protected AbstractProcessCommand(T component) {
    this.component = component;
  }

  @Override
  public final void run() {
    if (component.isEnabled()) {
      process();
    }
  }

  @Override
  public void runFor(int rounds) {
    int executions = 0;
    while (component.isEnabled() && executions < rounds) {
      run();
      executions += 1;
    }
  }

  @Override
  public abstract void process();

}
