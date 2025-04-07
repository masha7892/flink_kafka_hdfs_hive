package com;

import com.alibaba.fastjson.JSON;

import com.alibaba.fastjson.JSONObject;

import org.apache.flink.api.common.eventtime.WatermarkStrategy;



import org.apache.flink.api.common.serialization.SimpleStringSchema;




import org.apache.flink.connector.file.sink.FileSink;
import org.apache.flink.connector.kafka.source.KafkaSource;
import org.apache.flink.connector.kafka.source.enumerator.initializer.OffsetsInitializer;


import org.apache.flink.core.fs.Path;
import org.apache.flink.core.io.SimpleVersionedSerializer;
import org.apache.flink.orc.OrcSplitReaderUtil;
import org.apache.flink.orc.vector.RowDataVectorizer;
import org.apache.flink.orc.writer.OrcBulkWriterFactory;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.sink.filesystem.BucketAssigner;
import org.apache.flink.streaming.api.functions.sink.filesystem.PartFileInfo;
import org.apache.flink.streaming.api.functions.sink.filesystem.rollingpolicies.CheckpointRollingPolicy;
import org.apache.flink.table.data.GenericRowData;
import org.apache.flink.table.data.RowData;
import org.apache.flink.table.data.StringData;
import org.apache.flink.table.types.logical.*;
import org.apache.hadoop.conf.Configuration;
import org.apache.orc.TypeDescription;


import java.io.IOException;
import java.util.Properties;

public class example {
    public static void main(String[] args) throws Exception {
//        与 HDFS 交互时，程序将以hdfs用户的身份进行操作
        System.setProperty("HADOOP_USER_NAME", "hdfs");



//        获取 Flink 的流执行环境。
//        设置 Flink 作业的并行度为 1，即所有操作都将在一个并行任务中执行
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);



//        启用检查点机制，检查点的间隔为 60000 毫秒（即 60 秒）
//        设置检查点模式为EXACTLY_ONCE，确保数据在发生故障时不会丢失也不会重复处理。
        env.enableCheckpointing(60000);
//        env.getCheckpointConfig().setCheckpointingMode(CheckpointingMode.EXACTLY_ONCE);




//        创建一个 Kafka 数据源,最早偏移量开始消费。
//        反序列化器为SimpleStringSchema，将 Kafka 消息反序列化为字符串。
        KafkaSource<String> source = KafkaSource.<String>builder()
                .setBootstrapServers("cdh01:9092")
                .setTopics("test")
                .setGroupId("my_group")
                .setStartingOffsets(OffsetsInitializer.earliest())
                .setValueOnlyDeserializer(new SimpleStringSchema())
                .build();



//        从 Kafka 数据源读取数据，不使用水印并为该数据源命名为a
        DataStreamSource<String> ds1 = env.fromSource(source, WatermarkStrategy.noWatermarks(), "a");




//        创建一个包含 5 个字段的GenericRowData对象。
//        使用 FastJSON 解析输入的 JSON 字符串，提取id、name、school、ds和ts字段的值。
//        最后将这些值设置到RowData对象的相应字段中，并返回该对象
        SingleOutputStreamOperator<RowData> ds2 = ds1.map(s->{
            GenericRowData row = new GenericRowData(5);
            JSONObject jsonObject = JSON.parseObject(s);

            Integer id = jsonObject.getJSONObject("info").getInteger("id");
            String name = jsonObject.getJSONObject("info").getString("name");
            String school = jsonObject.getString("school");
            String ds = jsonObject.getString("ds");
            Long ts = jsonObject.getLong("ts");

            row.setField(0,id);
            row.setField(1,StringData.fromString(name));
            row.setField(2, StringData.fromString(school));
            row.setField(3,StringData.fromString(ds));
            row.setField(4,ts);

            return row;
        });


        ds2.print();

//        orcTypes：定义了 ORC 文件中每个字段的逻辑类型，包括整数类型、可变字符类型和长整数类型。
//        fieldNames：定义了 ORC 文件中每个字段的名称。
        LogicalType[] orcTypes = {new IntType(), new VarCharType(20), new VarCharType(50), new VarCharType(50), new BigIntType()};
        String[] fieldNames = {"id", "name", "school", "ds", "ts"};


//        将 Flink 的逻辑类型转换为 ORC 的类型描述。
        TypeDescription typeDescription = OrcSplitReaderUtil.logicalTypeToOrcType(RowType.of(orcTypes, fieldNames));


//        RowDataVectorizer：用于将RowData对象转换为 ORC 文件所需的向量格式。
        RowDataVectorizer vectorizer = new RowDataVectorizer(typeDescription.toString(), orcTypes);


//        创建一个Properties对象，用于配置 ORC 文件的写入属性。
//        设置 ORC 文件的压缩格式为SNAPPY。
        Properties writeProps = new Properties();
        writeProps.setProperty("orc.compress","SNAPPY");

//        创建一个 Hadoop 配置对象，用于与 HDFS 交互。
        Configuration hadoopConf = new Configuration();

//        用于创建 ORC 文件的批量写入器。
        OrcBulkWriterFactory<RowData> factory = new OrcBulkWriterFactory<>(vectorizer, writeProps, hadoopConf);





//        创建一个文件接收器，将数据以批量格式写入 HDFS 的hdfs://cdh01:8020/flink/kafka路 径下。
        FileSink<RowData> sink = FileSink.forBulkFormat(new Path("hdfs://cdh01:8020/flink/flink/kafka"), factory)
//        判定何时关闭当前正在写入的文件，并且开启一个新文件用于后续数据的写入
                .withRollingPolicy(new CheckpointRollingPolicy<RowData, String>() {
//                    用于判断是否要基于检查点触发文件滚动
                    @Override
                    public boolean shouldRollOnCheckpoint(PartFileInfo<String> partFileState) {
                        return super.shouldRollOnCheckpoint(partFileState);
                    }
//                    用于判断是否要基于新事件触发文件滚动
                    @Override
                    public boolean shouldRollOnEvent(PartFileInfo<String> partFileInfo, RowData rowData) throws IOException {
                        return false;
                    }
//                    用于判断是否要基于处理时间触发文件滚动
                    @Override
                    public boolean shouldRollOnProcessingTime(PartFileInfo<String> partFileInfo, long l) throws IOException {
                        return false;
                    }
                })
//                60秒检查一次桶状态,对比withRollingPolicy的条件判断是否执行滚动
                .withBucketCheckInterval(6000L)
//                根据ds字段分区
                .withBucketAssigner(new BucketAssigner<RowData, String>() {
//                    从RowData中提取ds字段（索引3
                    @Override
                    public String getBucketId(RowData rowData, Context context) {
                        return rowData.getString(3).toString();
                    }

                    @Override
                    public SimpleVersionedSerializer<String> getSerializer() {
                        return new SimpleVersionedSerializer<String>() {
                            @Override
                            public int getVersion() {
                                return 0;
                            }

                            @Override
                            public byte[] serialize(String s) throws IOException {
                                return new byte[0];
                            }

                            @Override
                            public String deserialize(int i, byte[] bytes) throws IOException {
                                return null;
                            }
                        };
                    }
                })
                .build();

//        将转换后的数据写入到文件接收器中。
        ds2.sinkTo(sink);


        env.execute();
    }
}
