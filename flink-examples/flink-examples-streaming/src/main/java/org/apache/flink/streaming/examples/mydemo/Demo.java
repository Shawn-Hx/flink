package org.apache.flink.streaming.examples.mydemo;

import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.functions.ReduceFunction;
import org.apache.flink.api.java.functions.KeySelector;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.KeyedStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.sink.RichSinkFunction;
import org.apache.flink.streaming.api.functions.sink.SinkFunction;
import org.apache.flink.streaming.api.functions.source.SourceFunction;

import java.util.Random;

public class Demo {

    private static class DataSource implements SourceFunction<Tuple2<String, Integer>> {

        private volatile boolean running = true;

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

        public void cancel() {
            running = false;
        }
    }

    public static void main(String[] args) throws Exception { StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
//        env.disableOperatorChaining();
//        env.setParallelism(2);

        DataStream<Tuple2<String, Integer>> ds = env.addSource(new DataSource())
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
			.setParallelism(3)
		.addSink(new SinkFunction<Tuple2<String, Integer>>() {
			@Override
			public void invoke(Tuple2<String, Integer> value, Context context) {
                System.out.printf("Get:\t(%s, %d)%n", value.f0, value.f1);
			}
		})
			.setParallelism(3);
        
//        // source --> map --> sink
//        ds.map(new MapFunction<Tuple2<String, Integer>, Tuple2<String, Integer>>() {
//            @Override
//            public Tuple2<String, Integer> map(Tuple2<String, Integer> tuple) {
//                return Tuple2.of(tuple.f0 + "x", tuple.f1 * 10);
//            }
//        })
//			.setParallelism(1)
//			.slotSharingGroup("1")
//        .addSink(new SinkFunction<Tuple2<String, Integer>>() {
//            public void invoke(Tuple2<String, Integer> value, Context context) {
//                System.out.printf("Get:\t(%s, %d)%n", value.f0, value.f1);
//            }
//        })
//			.setParallelism(1)
//			.slotSharingGroup("1")
//		;

//        // source --> map --> sink
//        ds.map(new MapFunction<Tuple2<String, Integer>, Tuple2<String, Integer>>() {
//            @Override
//            public Tuple2<String, Integer> map(Tuple2<String, Integer> tuple) {
//                return Tuple2.of(tuple.f0 + "y", -tuple.f1);
//            }
//        }).setParallelism(1).slotSharingGroup("2")
//        .addSink(new SinkFunction<Tuple2<String, Integer>>() {
//            @Override
//            public void invoke(Tuple2<String, Integer> value, Context context) {
//                System.out.printf("Get:\t(%s, %d)%n", value.f0, value.f1);
//            }
//        }).setParallelism(1).slotSharingGroup("2");

        env.execute();
    }
}
