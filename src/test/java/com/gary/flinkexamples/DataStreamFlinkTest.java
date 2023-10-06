package com.gary.flinkexamples;

import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.timestamps.BoundedOutOfOrdernessTimestampExtractor;
import org.apache.flink.streaming.api.windowing.assigners.TumblingEventTimeWindows;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.junit.jupiter.api.Test;

import java.time.ZonedDateTime;

class DataStreamFlinkTest {
    private final StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

    /**
     * Data Steam test - stream of events
     * @throws Exception
     */
    @Test
    void givenStreamOfEventsWhenProcessEventsThenPrintResultsOnSinkOperation() throws Exception{

        DataStream<String> text = env.fromElements("This is a first sentence", "This is a second sentence with a one word");

        SingleOutputStreamOperator<String> upperCase = text.map(String::toUpperCase);

        upperCase.print();

        // when
        env.execute();
    }

    /**
     * Windowing of Events
     *
     * Warning: chill-java api required for objects mapping in array list serializer
     * @throws Exception
     */
    @Test
    void givenStreamOfEventsWhenProcessEventsThenShouldApplyWindowingOnTransformation() throws Exception {
        SingleOutputStreamOperator<Tuple2<Integer, Long>> windowed = env.fromElements(
                new Tuple2<>(16, ZonedDateTime.now().plusMinutes(25).toInstant().getEpochSecond()),
                        new Tuple2<>(15, ZonedDateTime.now().plusMinutes(2).toInstant().getEpochSecond()))
                .assignTimestampsAndWatermarks(new BoundedOutOfOrdernessTimestampExtractor<Tuple2<Integer, Long>>(Time.seconds(20)) {
                    @Override
                    public long extractTimestamp(Tuple2<Integer, Long> element) {
                        return element.f1 * 1000;
                    }
                });

        SingleOutputStreamOperator<Tuple2<Integer, Long>> reduced = windowed.windowAll(
                TumblingEventTimeWindows.of(Time.seconds(5))).maxBy(0, true);

        reduced.print();

        // when
        env.execute();
    }
}