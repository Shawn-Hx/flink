package org.apache.flink.streaming.examples.mydemo;

import org.apache.flink.api.common.functions.ReduceFunction;
import org.apache.flink.api.java.functions.KeySelector;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.sink.SinkFunction;

public class KeyByReduceDemo {

	public static void main(String[] args) throws Exception {
		StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

		DataStream<Tuple2<String, Integer>> ds = env.addSource(new MyDataSource())
			.setParallelism(1)
//			.slotSharingGroup("1")
			;

		ds.keyBy(new KeySelector<Tuple2<String, Integer>, String>() {
			@Override
			public String getKey(Tuple2<String, Integer> value) {
				return value.f0;
			}
		}).reduce(new ReduceFunction<Tuple2<String, Integer>>() {
			@Override
			public Tuple2<String, Integer> reduce(Tuple2<String, Integer> value1, Tuple2<String, Integer> value2) {
				return Tuple2.of(value1.f0, value1.f1 + value2.f1);
			}
		})
			.setParallelism(2)
			.addSink(new SinkFunction<Tuple2<String, Integer>>() {
				@Override
				public void invoke(Tuple2<String, Integer> value, SinkFunction.Context context) {
					System.out.printf("Get:\t(%s, %d)%n", value.f0, value.f1);
				}
			})
			.setParallelism(1);

		env.execute();
	}
}
