package scheduling.impl;

import java.io.FileWriter;
import java.io.IOException;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.LongAdder;

import org.apache.commons.csv.CSVFormat;
import org.apache.commons.csv.CSVPrinter;

import operator.Operator;
import scheduling.ActiveThread;
import scheduling.TaskPool;

public class WorkerThread extends ActiveThread {
	private final TaskPool<Operator<?, ?>> availableTasks;
	private long interval;
	private final TimeUnit unit;
	private long runs = 0;
	private long time = 0;
	private Map<String, LongAdder> operatorCalls = new HashMap<>();
	private Map<String, LongAdder> executionTimes = new HashMap<>();
	private Map<String, LongAdder> schedulingTimes = new HashMap<>();
	private static long number = 0;

	private Map<String, Long> emptyRuns = new HashMap<>();
	private final long index;

	public WorkerThread(TaskPool<Operator<?, ?>> availableTasks, long interval, TimeUnit unit) {
		this.availableTasks = availableTasks;
		this.interval = interval;
		this.unit = unit;
		this.index = number;
		number++;
	}

	@Override
	public void doRun() {
		long start = System.nanoTime();
		long schedulingTime = 0;
		Operator<?, ?> task = availableTasks.getNext(index);
		operatorCalls.computeIfAbsent(task.getId(), k -> new LongAdder()).increment();
		schedulingTime += System.nanoTime() - start;
		runs++;
		// System.out.format("+ [T%d] %s%n", getId(), task);
		long runUntil = System.nanoTime() + unit.toNanos(interval);
		long execStart = System.currentTimeMillis();
		emptyRuns.putIfAbsent(task.getId(), 0L);
		emptyRuns.compute(task.getId(), (k, v) -> task.hasInput() ? v : v + 1);
		while (System.nanoTime() < runUntil && task.hasInput()) {
			task.run();
		}
		executionTimes.computeIfAbsent(task.getId(), k -> new LongAdder()).add(System.currentTimeMillis() - execStart);
		start = System.nanoTime();
		// System.out.format("- [T%d] %s%n", getId(), task);
		availableTasks.put(task);
		schedulingTime += System.nanoTime() - start;
		time += schedulingTime;
		schedulingTimes.computeIfAbsent(task.getId(), k -> new LongAdder()).add(schedulingTime);
	}

	@Override
	public void disable() {
		super.disable();
		System.out.format("T%d Scheduling Average = %3.2f ns%n", getId(), time / (double) runs);
		try {
			CSVPrinter printer = new CSVPrinter(new FileWriter(String.format("report/T%d.exec.csv", getId())),
					CSVFormat.DEFAULT);
			for (String key : operatorCalls.keySet()) {
				printer.printRecord(key, operatorCalls.get(key).longValue(), executionTimes.get(key),
						schedulingTimes.get(key), emptyRuns.get(key));
			}
			printer.close();
		} catch (IOException e) {
			e.printStackTrace();
		}
	}

}
