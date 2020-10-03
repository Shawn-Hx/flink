package org.apache.flink.streaming.examples.mydemo;

import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.sink.SinkFunction;

public class MapDemo {

    public static void main(String[] args) throws Exception {
    	StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
//        env.disableOperatorChaining();
//        env.setParallelism(2);

		DataStream<Tuple2<String, Integer>> ds = env.addSource(new MyDataSource())
			.setParallelism(1)
//			.slotSharingGroup("1")
			;

        // source --> map --> sink
        ds.map(new MapFunction<Tuple2<String, Integer>, Tuple2<String, Integer>>() {
            @Override
            public Tuple2<String, Integer> map(Tuple2<String, Integer> tuple) {
                return Tuple2.of(tuple.f0 + "x", tuple.f1 * 10);
            }
        })
			.setParallelism(2)
//			.slotSharingGroup("1")
        .addSink(new SinkFunction<Tuple2<String, Integer>>() {
            public void invoke(Tuple2<String, Integer> value, Context context) {
                System.out.printf("Get:\t(%s, %d)%n", value.f0, value.f1);
            }
        })
			.setParallelism(1)
//			.slotSharingGroup("1")
		;

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
