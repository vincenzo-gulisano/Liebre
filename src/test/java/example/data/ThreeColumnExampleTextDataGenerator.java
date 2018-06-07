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

  private static final int KEY = 2;
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
    return 10000;
  }

}
