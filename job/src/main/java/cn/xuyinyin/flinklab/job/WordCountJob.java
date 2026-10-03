package cn.xuyinyin.flinklab.job;

import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.util.Collector;

public final class WordCountJob {
    private WordCountJob() {}

    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment environment = StreamExecutionEnvironment.getExecutionEnvironment();
        DataStream<String> words = environment.fromElements(
                "scala zio flink",
                "flink operator observes desired state",
                "scala zio controls side effects");

        words.flatMap(new Tokenizer()).keyBy(value -> value.f0).sum(1).print();
        environment.execute("zio-flink-operator-lab-word-count");
    }

    private static final class Tokenizer implements FlatMapFunction<String, Tuple2<String, Integer>> {
        @Override
        public void flatMap(String value, Collector<Tuple2<String, Integer>> out) {
            for (String token : value.toLowerCase().split("\\s+")) {
                out.collect(Tuple2.of(token, 1));
            }
        }
    }
}
