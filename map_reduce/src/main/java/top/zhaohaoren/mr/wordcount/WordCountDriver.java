package top.zhaohaoren.mr.wordcount;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import java.io.IOException;

public class WordCountDriver {

    public static void main(String[] args) throws IOException, ClassNotFoundException, InterruptedException {
        // 1 获取配置信息以及封装任务
        Configuration conf = new Configuration();
        Job job = Job.getInstance(conf);

        // 2 设置jar加载路径
        job.setJarByClass(WordCountDriver.class);

        // 3 设置map和reduce类
        job.setMapperClass(WordCountMapper.class);
        job.setReducerClass(WordCountReducer.class);

        // 4 设置map输出kv类型
        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(IntWritable.class);

        // 5 设置最终输出kv类型
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(IntWritable.class);

        // 6 设置输入和输出路径
        FileInputFormat.setInputPaths(job, new Path("/Users/zhaohaoren/workspace/code/mine/JavaProjects/BigData/map_reduce/src/main/java/top/zhaohaoren/mr/wordcount/content"));
        FileOutputFormat.setOutputPath(job, new Path("/Users/zhaohaoren/workspace/code/mine/JavaProjects/BigData/map_reduce/src/main/java/top/zhaohaoren/mr/wordcount/output/"));

        // 7 提交
        // job.submit(); 不使用
        boolean result = job.waitForCompletion(true);

        System.exit(result ? 0 : 1);
    }
}
