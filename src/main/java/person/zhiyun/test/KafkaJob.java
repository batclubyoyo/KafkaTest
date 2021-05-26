package person.zhiyun.test;

import java.util.Properties;

import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.JSONObject;

import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.streaming.api.CheckpointingMode;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.CheckpointConfig;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer010;

/**
 * KafkaJob
 *
 */
public class KafkaJob
{
    public static void main( String[] args ) throws Exception
    {
        System.out.println("aaaaaaaaaaaaaa");

		// set up the streaming execution environment
		final StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        //设置并行度，为了方便测试，查看消息的顺序，这里设置为1，可以更改为多并行度
        env.setParallelism(1);
        //checkpoint设置
        //每隔10s进行启动一个检查点【设置checkpoint的周期】
        env.enableCheckpointing(10000);
        //设置模式为：exactly_one，仅一次语义
        env.getCheckpointConfig().setCheckpointingMode(CheckpointingMode.EXACTLY_ONCE);
        //确保检查点之间有1s的时间间隔【checkpoint最小间隔】
        env.getCheckpointConfig().setMinPauseBetweenCheckpoints(1000);
        //检查点必须在10s之内完成，或者被丢弃【checkpoint超时时间】
        env.getCheckpointConfig().setCheckpointTimeout(10000);
        //同一时间只允许进行一次检查点
        env.getCheckpointConfig().setMaxConcurrentCheckpoints(1);
        //表示一旦Flink程序被cancel后，会保留checkpoint数据，以便根据实际需要恢复到指定的checkpoint
        env.getCheckpointConfig().enableExternalizedCheckpoints(CheckpointConfig.ExternalizedCheckpointCleanup.RETAIN_ON_CANCELLATION);

		Properties properties = new Properties();
		properties.setProperty("bootstrap.servers", "yx3326:6667,yx3328:6667,yx3329:6667");
		properties.setProperty("group.id", "zhiyun-flink-kafka-test_two");

		// 1,abc,100 类似这样的数据，当然也可以是很复杂的json数据，去做解析
		FlinkKafkaConsumer010<String> consumer = new FlinkKafkaConsumer010<>("DockerOracle.MD61.TESTUSER", new SimpleStringSchema(), properties);
		consumer.setStartFromEarliest();

        System.out.println("bbbbbbb");

		DataStream<String> stream = env.addSource(consumer);
//        stream.print();
		 DataStream<String> sourceStream = stream.map(value -> {
		 	JSONObject jsonObj = JSON.parseObject(value);
            String payload = jsonObj.getString("payload");
//            System.out.println(payload);
		 	return payload;
		 });
         sourceStream.print();

		// execute program
		env.execute("Flink Streaming Java API Skeleton");

    }
}
