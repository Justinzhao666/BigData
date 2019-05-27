package mr.hdfs2hbase;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.mapreduce.TableMapReduceUtil;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

public class HdfsDriver extends Configuration implements Tool {

    Configuration conf = null;

    @Override
    public int run(String[] strings) throws Exception {
        // 这里写具体的环境配置东西

        //获取job
        Job instance = Job.getInstance(conf);
        //指定driver类
        instance.setJarByClass(HdfsDriver.class);
        //指定mapper
        instance.setMapperClass(HdfsMapper.class);
        instance.setMapOutputKeyClass(NullWritable.class);
        instance.setMapOutputValueClass(Put.class);
        //指定reducer
//        instance.setReducerClass(FruitReducer.class);
        TableMapReduceUtil.initTableReducerJob("fruit_mr2", HdfsReducer.class, instance);

        //设置输入路径
        FileInputFormat.setInputPaths(instance, strings[0]);

        //提交任务
        return instance.waitForCompletion(true) ? 1 : 0;

    }

    @Override
    public void setConf(Configuration configuration) {
        this.conf = configuration;
    }

    @Override
    public Configuration getConf() {
        return conf;
    }

    public static void main(String[] args) throws Exception {
        Configuration configuration = HBaseConfiguration.create();
        System.exit(ToolRunner.run(configuration, new HdfsDriver(), args));
    }
}
