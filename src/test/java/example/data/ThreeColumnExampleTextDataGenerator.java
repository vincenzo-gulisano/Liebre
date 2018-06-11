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

package example.data;

import java.util.Arrays;
import java.util.List;
import java.util.Random;

/**
 * Generate random (timestamp, key, value) CSV data.
 *
 * @author palivosd
 */
public class ThreeColumnExampleTextDataGenerator extends ExampleTextDataGenerator {

  private static final int KEY = 1;
  private static final String OUTPUT_FILE = String.format("report/dummy_data_%d.csv", KEY);
  private final Random rand = new Random();

  public static void main(String[] args) throws Exception {
    ExampleTextDataGenerator generator = new ThreeColumnExampleTextDataGenerator();
    generator.generate(OUTPUT_FILE);
  }

  @Override
  protected List<String> getNextRecord() {
    return Arrays
        .asList(String.valueOf(System.currentTimeMillis()), String.valueOf(KEY), String.valueOf(rand.nextInt(100)));
  }

  @Override
  protected int numberOfLinesToGenerate() {
    return 1000000;
  }

}
