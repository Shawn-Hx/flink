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

import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.sink.SinkFunction;

import java.util.concurrent.TimeUnit;

/**
 * Simple streaming job.
 */
public class StreamingJob {

	public static void main(String[] args) throws Exception {
		StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

		DataStream<Tuple2<String, Integer>> ds = env
			.addSource(new MyDataSource(TimeUnit.SECONDS, 1))
			.setParallelism(1);

		// source --> map --> sink
		ds.map(new MapFunction<Tuple2<String, Integer>, Tuple2<String, Integer>>() {
			@Override
			public Tuple2<String, Integer> map(Tuple2<String, Integer> tuple) {
				return Tuple2.of(tuple.f0 + "x", tuple.f1 * 10);
			}
		}).setParallelism(2)
		.addSink(new SinkFunction<Tuple2<String, Integer>>() {
			public void invoke(Tuple2<String, Integer> value, Context context) {
//				System.out.printf("Get:\t(%s, %d)%n", value.f0, value.f1);
			}
		}).setParallelism(1);

		env.execute("huangxiao's simple job");
	}

}
