package com.xinchao.flink.stream;

import com.xinchao.flink.constant.KafkaConstant;
import com.xinchao.flink.dto.HeartbeatDTO;
import com.xinchao.flink.util.DateUtils;
import com.xinchao.flink.util.PropertiesUtils;
import org.apache.flink.api.common.functions.FilterFunction;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.functions.ReduceFunction;
import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.api.java.functions.KeySelector;
import org.apache.flink.api.java.tuple.Tuple;
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.streaming.api.TimeCharacteristic;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.WindowedStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.timestamps.AscendingTimestampExtractor;
import org.apache.flink.streaming.api.windowing.assigners.EventTimeSessionWindows;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Properties;

/**
 * @author dxy
 */
public class HeartbeatJob {
    /**
     * 日志
     */
    private static Logger logger = LoggerFactory.getLogger(HeartbeatJob.class);

    public static void main(String[] args) {
        // get StreamExecutionEnvironment
        final StreamExecutionEnvironment streamEnvironment = StreamExecutionEnvironment.getExecutionEnvironment();
        streamEnvironment.setStreamTimeCharacteristic(TimeCharacteristic.EventTime);

        // Kafka properties
        Properties properties = new Properties();
        properties.setProperty(KafkaConstant.BOOTSTRAP_SERVERS, PropertiesUtils.getValueByKey(KafkaConstant.BOOTSTRAP_SERVERS));
        properties.setProperty(KafkaConstant.GROUP_ID, PropertiesUtils.getValueByKey(KafkaConstant.GROUP_ID));
        // Kafka Consumer
        FlinkKafkaConsumer<String> kafkaConsumer = new FlinkKafkaConsumer<>(PropertiesUtils.getValueByKey(KafkaConstant.TOPIC), new SimpleStringSchema(), properties);

        DataStreamSource<String> dataStream = streamEnvironment.addSource(kafkaConsumer);

        dataStream.map(str -> {
            String[] arr = str.split(",");
            HeartbeatDTO dto = new HeartbeatDTO();
            if (arr != null && arr.length > 0) {
                dto.setTime(arr[0]);
                dto.setDeviceCode(arr[1]);
            }
            return dto;
        }).assignTimestampsAndWatermarks(new AscendingTimestampExtractor<HeartbeatDTO>() {
            @Override
            public long extractAscendingTimestamp(HeartbeatDTO element) {
                return DateUtils.stringDateTimeToLong(element.getTime(), "yyyy-MM-dd HH:mm:ss");
            }
        }).map((MapFunction<HeartbeatDTO, Tuple3<String, String, Integer>>) value -> new Tuple3<>(value.getDeviceCode(), value.getTime(), 1))
        .keyBy((KeySelector<Tuple3<String, String, Integer>, String>) value -> value.f0)
        .window(EventTimeSessionWindows.withGap(Time.minutes(5)))
        .allowedLateness(Time.minutes(1))
        .reduce((ReduceFunction<Tuple3<String, String, Integer>>) (value1, value2) -> new Tuple3<>(value1.f0, value1.f1, value1.f2 + value2.f2))
        .filter((FilterFunction<Tuple3<String, String, Integer>>) value -> value.f2 < 2)
        .print();

//        WindowedStream<HeartbeatDTO, Tuple, TimeWindow> window = streamMap.keyBy("deviceCode")
//                .window(EventTimeSessionWindows.withGap(Time.minutes(5)))
//                .allowedLateness(Time.seconds(10));

        try {
            streamEnvironment.execute("计算心跳数据");
        } catch (Exception e) {
            e.printStackTrace();
            logger.error("计算心跳数据", e);
        }
    }

}
