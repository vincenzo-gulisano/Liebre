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

package query;

import component.Component;
import component.source.Source;
import java.util.Collection;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

public class QueryCostEstimator implements Runnable {

  public static final int SAMPLE_FREQUENCY_MILLIS = 10000;
  private static final Logger LOG = LogManager.getLogger();
  private final Query query;
  private final boolean scheduling;

  public QueryCostEstimator(Query query, boolean scheduling) {
    this.query = query;
    this.scheduling = scheduling;
  }

  @Override
  public void run() {
    if (!scheduling) {
      return;
    }
    while (!Thread.currentThread().isInterrupted()) {
      try {
        Thread.currentThread().sleep(SAMPLE_FREQUENCY_MILLIS);
      } catch (InterruptedException e) {
        return;
      }
      Collection<Source<?>> sources = query.sources();
      double totalCost = 0;
      double totalUtilization = 0;
      double totalRate = 0;
      for (Source<?> source : sources) {
        double sourceCost = globalAverageCost(source) / 1000000000;
        double sourceRate = source.getRate()*1000;
//        LOG.info("Source {} max rate = {} t/s", source, 1 / sourceCost);
//        LOG.info("Source {} current rate = {} t/s", source, sourceRate);
        totalCost += sourceCost;
        totalRate += sourceRate;
        totalUtilization += 100*(sourceRate * sourceCost);
      }
      LOG.info("Current Average Rate = {} t/s", totalRate / sources.size());
      LOG.info("Current Maximum Rate = {} t/s", 1 / totalCost);
      LOG.info("Current Utilization = {}%", String.format("%3.2f", totalUtilization));
    }
  }

  private double globalAverageCost(Component component) {
    double globalAverageCost = component.getCost();
    double selectivity = component.getSelectivity();
    for (Component downstream : component.getDownstream()) {
      globalAverageCost += selectivity * globalAverageCost(downstream);
    }
    return globalAverageCost;

  }


}
