/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.flink.streaming.examples.mydemo;

import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.functions.source.SourceFunction;

import java.util.Random;
import java.util.concurrent.TimeUnit;

/**
 * My DataSource.
 */
public class MyDataSource implements SourceFunction<Tuple2<String, Integer>> {
	private volatile boolean running = true;
	private final TimeUnit timeunit;
	private final long timeout;

	public MyDataSource(TimeUnit timeunit, long timeout) {
		this.timeunit = timeunit;
		this.timeout = timeout;
	}

	@Override
	public void run(SourceContext<Tuple2<String, Integer>> sourceContext) throws Exception {
		Random random = new Random();
		while (running) {
			this.timeunit.sleep(timeout);
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
