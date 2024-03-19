package org.example;

import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.connectors.redis.RedisSink;
import org.apache.flink.streaming.connectors.redis.common.config.FlinkJedisPoolConfig;
import org.apache.flink.streaming.connectors.redis.common.mapper.RedisCommand;
import org.apache.flink.streaming.connectors.redis.common.mapper.RedisCommandDescription;
import org.apache.flink.streaming.connectors.redis.common.mapper.RedisMapper;
import org.apache.flink.util.Collector;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class WordCountRedisExample {
    private static final Logger logger = LoggerFactory.getLogger(WordCountRedisExample.class);

    public static void main(String[] args) throws Exception {
        // 创建执行环境
//        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        StreamExecutionEnvironment env = StreamExecutionEnvironment.createLocalEnvironment();
//        env.setParallelism(1);  // 设置并行度为1

//        // 从本地的txt文件中读取数据
//        String filePath = "/home/hay/hay/flink/flink-1.18.1/file.txt";
//        DataStream<String> dataStream = env.readTextFile(filePath);
        // 从现有字符串创建数据流
        String inputString = "hello world hello flink hello java";
        DataStream<String> dataStream = env.fromElements(inputString);

        // 进行单词计数
        DataStream<Tuple2<String, Integer>> wordCounts = dataStream
                .flatMap(new FlatMapFunction<String, Tuple2<String, Integer>>() {
                    @Override
                    public void flatMap(String line, Collector<Tuple2<String, Integer>> out) {
                        String[] words = line.split("\\s+");
                        for (String word : words) {
                            out.collect(new Tuple2<>(word, 1));
                        }
                    }
                })
                .keyBy(0)
                .sum(1);

        // 打印单词计数结果
        wordCounts.print();

//        // 将结果写入 Redis
//        FlinkJedisPoolConfig jedisPoolConfig = new FlinkJedisPoolConfig.Builder()
//                .setHost("localhost")
//                .setPort(6379)
//                .build();
//
//        wordCounts.addSink(new RedisSink<>(jedisPoolConfig, new WordCountRedisMapper()));

        // 执行作业
        env.execute("Word Count Redis Example");
    }

    // 自定义 RedisMapper,将单词计数结果写入 Redis
    public static class WordCountRedisMapper implements RedisMapper<Tuple2<String, Integer>> {
        @Override
        public RedisCommandDescription getCommandDescription() {
            return new RedisCommandDescription(RedisCommand.SET);
        }

        @Override
        public String getKeyFromData(Tuple2<String, Integer> data) {
            return data.f0;
        }

        @Override
        public String getValueFromData(Tuple2<String, Integer> data) {
            return data.f1.toString();
        }
    }
}