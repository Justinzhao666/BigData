package mr;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.io.ImmutableBytesWritable;
import org.apache.hadoop.hbase.mapreduce.TableMapReduceUtil;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

/**
 * 官方建议这样写driver
 */
public class FruitDriver extends Configuration implements Tool {

    Configuration conf = null;

    @Override
    public int run(String[] strings) throws Exception {
        // 这里写具体的环境配置东西

        //获取job
        Job instance = Job.getInstance(conf);
        //指定driver类
        instance.setJarByClass(FruitDriver.class);
        //指定mapper
//        instance.setMapperClass(FruitMapper.class); hbase 封装了这个util
        TableMapReduceUtil.initTableMapperJob("fruit", new Scan(), FruitMapper.class, ImmutableBytesWritable.class, Put.class, instance);
        //指定reducer
//        instance.setReducerClass(FruitReducer.class);
        TableMapReduceUtil.initTableReducerJob("fruit_mr", FruitReducer.class, instance);
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
        ToolRunner.run(configuration, new FruitDriver(), args);
    }

}
