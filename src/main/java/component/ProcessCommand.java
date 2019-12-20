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
 * Command pattern to decouple the processing logic of components from the specific subclasses.
 *
 * @author palivosd
 */
public interface ProcessCommand extends Runnable {

  void updateMetrics();

  double getSelectivity();

  double getCost();

  double getRate();

  /**
   * The main processing function of the component
   */
  void process();

  @Override
  void run();

  /**
   * Execute {@link #run()} for the given number of repetitions.
   *
   * @param rounds The number of times that this command will be run.
   * @return {@code true} if the command processed at least one tuple
   */
  boolean runFor(int rounds);

}