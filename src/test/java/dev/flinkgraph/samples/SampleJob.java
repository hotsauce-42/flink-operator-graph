package dev.flinkgraph.samples;

import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.ProcessFunction;
import org.apache.flink.streaming.api.functions.sink.v2.DiscardingSink;
import org.apache.flink.util.Collector;
import org.apache.flink.util.OutputTag;

/** Two sources, union, keyBy, side output, rebalance and two sinks: covers what the generator must draw. */
public class SampleJob {

    public static final OutputTag<String> ERRORS = new OutputTag<>("errors", Types.STRING);

    public static void build(StreamExecutionEnvironment env) {
        DataStream<String> orders = env.fromData("a", "b").name("Orders");
        DataStream<String> returns = env.fromData("c").name("Returns");

        SingleOutputStreamOperator<String> validated = orders.union(returns)
                .process(new ProcessFunction<String, String>() {
                    @Override
                    public void processElement(String value, Context ctx, Collector<String> out) {
                        if (value.isEmpty()) ctx.output(ERRORS, value); else out.collect(value);
                    }
                }).name("Validate");

        validated.keyBy(s -> s).map(String::toUpperCase).name("Enrich")
                .rebalance().sinkTo(new DiscardingSink<>()).name("Warehouse");

        validated.getSideOutput(ERRORS).sinkTo(new DiscardingSink<>()).name("Dead Letter");
    }

    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        build(env);
        env.execute("sample-job");
    }
}
