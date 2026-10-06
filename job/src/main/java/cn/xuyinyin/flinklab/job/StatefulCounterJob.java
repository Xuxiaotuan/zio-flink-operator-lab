package cn.xuyinyin.flinklab.job;

import org.apache.flink.api.common.state.ValueState;
import org.apache.flink.api.common.state.ValueStateDescriptor;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.source.RichParallelSourceFunction;
import org.apache.flink.api.common.functions.RichFlatMapFunction;
import org.apache.flink.util.Collector;

/**
 * Long-running stateful workload used only by the end-to-end control-plane
 * verification. Each emitted value advances keyed ValueState and is printed so
 * a savepoint restore can be checked from JobManager logs.
 */
public final class StatefulCounterJob {
    private StatefulCounterJob() {}

    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment environment = StreamExecutionEnvironment.getExecutionEnvironment();
        environment.enableCheckpointing(5000L);
        DataStream<Long> ticks = environment.addSource(new TickerSource()).name("e2e-ticker");
        ticks.keyBy(value -> "single-key")
                .flatMap(new CounterState())
                .name("e2e-counter-state")
                .print();
        environment.execute("zio-flink-stateful-counter");
    }

    private static final class TickerSource extends RichParallelSourceFunction<Long> {
        private volatile boolean running = true;

        @Override
        public void run(SourceContext<Long> context) throws Exception {
            long sequence = 0L;
            while (running) {
                synchronized (context.getCheckpointLock()) {
                    context.collect(++sequence);
                }
                Thread.sleep(1000L);
            }
        }

        @Override
        public void cancel() {
            running = false;
        }
    }

    private static final class CounterState extends RichFlatMapFunction<Long, String> {
        private transient ValueState<Long> total;

        @Override
        public void open(Configuration parameters) {
            total = getRuntimeContext().getState(new ValueStateDescriptor<>("total", Long.class));
        }

        @Override
        public void flatMap(Long value, Collector<String> out) throws Exception {
            long next = (total.value() == null ? 0L : total.value()) + value;
            total.update(next);
            String result = "stateful-counter=" + next;
            System.out.println(result);
            out.collect(result);
        }
    }
}
