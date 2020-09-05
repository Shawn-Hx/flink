package org.apache.flink.streaming.examples.mydemo;

import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.functions.source.SourceFunction;

import java.util.Random;

public class MyDataSource implements SourceFunction<Tuple2<String, Integer>> {
	private volatile boolean running = true;

	@Override
	public void run(SourceContext<Tuple2<String, Integer>> sourceContext) throws Exception {
		Random random = new Random();
		while (running) {
			Thread.sleep(2000);
			String key = "class" + (char) ('A' + random.nextInt(3));
			int value = random.nextInt(5) + 1;
			System.out.printf("Emit:\t(%s, %d)%n", key, value);
			sourceContext.collect(Tuple2.of(key, value));
		}
	}

	@Override
	public void cancel() {
		running = false;
	}
}
