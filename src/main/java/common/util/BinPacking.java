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

package common.util;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.Comparator;
import java.util.List;
import java.util.stream.IntStream;
import org.apache.commons.lang3.Validate;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

//TODO: Heavy refactor if we end up using it
public enum BinPacking {
  INSTANCE;

  private static final Logger LOGGER = LogManager.getLogger();

  public <T extends Packable> List<List<T>> split(List<T> values, final int binNumber) {
    final double maxCapacity = 1.0;
    Validate.isTrue(sum(values) <= binNumber, "Sum of percentages is higher than bin number!");
    List<T> tiny = new ArrayList<>();
    List<T> small = new ArrayList<>();
    List<T> medium = new ArrayList<>();
    List<T> large = new ArrayList<>();
    for (T p : values) {
      double value = p.getValue();
      if (value > maxCapacity / 2) {
        large.add(p);
      } else if (value > maxCapacity / 3) {
        medium.add(p);
      } else if (value > maxCapacity / 6) {
        small.add(p);
      } else {
        tiny.add(p);
      }
    }
    large.sort(Comparator.comparing(Packable::getValue));
    medium.sort(Comparator.comparing(Packable::getValue));
    small.sort( Comparator.comparing(Packable::getValue));
    tiny.sort(Comparator.comparing(Packable::getValue));
    List<T>[] bins = new List[binNumber];
    for (int i = 0; i < binNumber; i++) {
      bins[i] = new ArrayList<>();
    }
    boolean[] containsMediumItem = new boolean[binNumber];

    // Place large items
    Validate.validState(large.size() <= bins.length, "Something is wrong with the values you submitted!");
    for (int i = 0; i < large.size(); i++) {
      bins[i].add(large.get(i));
    }
    // Place medium items
    for (int i = 0; i < bins.length; i++) {
      T smallestThatFits = smallestThatFits(medium, bins[i], maxCapacity);
      if (smallestThatFits != null) {
        bins[i].add(smallestThatFits);
        containsMediumItem[i] = true;
      }
    }

    // Place two small items
    for (int i = bins.length - 1; i >= 0; i--) {
      if (small.size() == 0) {
        break;
      }
      if (containsMediumItem[i]) {
        continue;
      }
      if (small.size() == 1 && small.get(0).getValue() <= capacity(bins[i], maxCapacity)) {
        bins[i].add(small.remove(0));
      } else if (small.size() >= 2 && twoSmallestFit(small, bins[i], maxCapacity)) {
        bins[i].add(small.remove(0));
        bins[i].add(smallestThatFits(small, bins[i], maxCapacity));
      }
    }

    // Place everything else (within bounds)
    List<T> combined = all(tiny, small, medium);
    for (int i = 0; i < bins.length; i++) {
      while (!combined.isEmpty() && combined.get(0).getValue() < capacity(bins[i], maxCapacity)) {
        bins[i].add(combined.remove(0));
      }
    }

    // Greedy approach for the rest (ignore bounds)
    fillRemaining(bins, combined);
    return Arrays.asList(bins);
  }

  private <T extends Packable> void fillRemaining(List<T>[] bins, List<T> combined) {
    while (!combined.isEmpty()) {
      int idx = minimumSumBin(bins);
      bins[idx].add(combined.remove(combined.size() - 1));
    }
  }

  private int minimumSumBin(List<? extends Packable>[] bins) {
    return IntStream.range(0, bins.length).reduce((i, j) -> sum(bins[i]) > sum(bins[j]) ? j : i)
        .getAsInt();
  }

  public <T extends Packable> T smallestThatFits(List<T> values, List<T> bin, double size) {
    int idx = Collections
        .binarySearch(values, new PackableImpl(capacity(bin, size)),
            Comparator.comparing(Packable::getValue));
    int actualIndex;
    if (idx >= -1) {
      actualIndex = idx;
    } else {
      actualIndex = Math.abs(idx) - 2;
    }
    if (actualIndex < 0) {
      return null;
    }
    return values.remove(actualIndex);
  }

  public <T extends Packable> boolean twoSmallestFit(List<T> values, List<T> bin, double size) {
    if (values.size() < 2) {
      throw new IllegalStateException();
    }
    return values.get(0).getValue() + values.get(1).getValue() <= capacity(bin, size);
  }

  public double capacity(List<? extends Packable> bin, double size) {
    return size - sum(bin);
  }

  public <T extends Packable> List<T> all(List<T>... lists) {
    List<T> all = new ArrayList<>();
    for (List<T> list : lists) {
      all.addAll(list);
    }
    return all;
  }

  public void print(List<? extends Packable>[] bins) {
    LOGGER.debug("--- BINS ---");
    for (int i = 0; i < bins.length; i++) {
      LOGGER.debug("{}: {} -> {}", i, bins[i], sum(bins[i]));
    }
  }

  public double sum(List<? extends Packable> values) {
    return values.stream().mapToDouble(x -> x.getValue()).sum();
  }

  public interface Packable {

    double getValue();
  }

  private static class PackableImpl implements Packable {

    public final double value;

    public PackableImpl(double value) {
      this.value = value;
    }

    @Override
    public double getValue() {
      return value;
    }
  }

}
