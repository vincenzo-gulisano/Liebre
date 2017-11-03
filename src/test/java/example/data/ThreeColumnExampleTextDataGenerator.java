package example.data;

import java.util.Arrays;
import java.util.List;
import java.util.Random;

/**
 * Generate random (timestamp, key, value) CSV data.
 * 
 * @author palivosd
 *
 */
public class ThreeColumnExampleTextDataGenerator extends ExampleTextDataGenerator {
	private final Random rand = new Random();

	public static void main(String[] args) throws Exception {
		ExampleTextDataGenerator generator = new ThreeColumnExampleTextDataGenerator();
		generator.generate(args[0]);
	}

	@Override
	protected List<String> getNextRecord() {
		return Arrays.asList(String.valueOf(System.currentTimeMillis()), String.valueOf(rand.nextInt(5)),
				String.valueOf(rand.nextInt(100)));
	}

	@Override
	protected int numberOfLinesToGenerate() {
		return 10000;
	}

}
