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

package common.component;

/**
 * Type of event processed in {@link Component#waitFor(EventType)} and Component{@link
 * #notify(ComponentState)}. Takes care of the actual delegation to the correct functions in {@link
 * ComponentState}.
 *
 * @author palivosd
 */
public enum EventType {
  READ {
    @Override
    public void wait(ComponentState state) {
      state.waitForRead();
    }

    @Override
    public void notify(ComponentState state) {
      state.notifyForRead();
    }
  },
  WRITE {
    @Override
    public void wait(ComponentState state) {
      state.waitForWrite();
    }

    @Override
    public void notify(ComponentState state) {
      state.notifyForWrite();
    }
  };


  /**
   * Set the given {@link ComponentState} to wait for this event type
   */
  public abstract void wait(ComponentState state);

  /**
   * Notify {@link ComponentState} to wait for this event type
   */
  public abstract void notify(ComponentState state);
}
