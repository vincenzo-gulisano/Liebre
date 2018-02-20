package common;

import java.util.Collections;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.function.BiFunction;

public class ExecutionLog<K, V> {

	// TODO: Maybe it's safe to have just a HashMap
	private final Map<K, V> log = new ConcurrentHashMap<>();
	private final BiFunction<? super V, ? super V, ? extends V> mergeFunction;

	public static <K> ExecutionLog<K, Long> cummulativeLong() {
		return new ExecutionLog<K, Long>((a, b) -> a + b);
	}

	public static <K> ExecutionLog<K, Double> cummulativeDouble() {
		return new ExecutionLog<K, Double>((a, b) -> a + b);
	}

	public static <K> ExecutionLog<K, Double> minDouble() {
		return new ExecutionLog<K, Double>(Math::min);
	}

	public static <K> ExecutionLog<K, Long> minLong() {
		return new ExecutionLog<K, Long>(Math::min);
	}

	public static <K> ExecutionLog<K, Double> maxDouble() {
		return new ExecutionLog<K, Double>(Math::max);
	}

	public static <K> ExecutionLog<K, Long> maxLong() {
		return new ExecutionLog<K, Long>(Math::max);
	}

	public ExecutionLog(BiFunction<? super V, ? super V, ? extends V> mergeFunction) {
		this.mergeFunction = mergeFunction;
	}

	public void record(K item, V value) {
		log.merge(item, value, mergeFunction);
	}

	public void reset() {
		log.clear();
	}

	public Map<K, V> get() {
		return Collections.unmodifiableMap(log);
	}

}
