package top.zhaohaoren.mr.wordcount;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;

/**
 * Mapper阶段
 * <输入key类型，输入value类型，输出key类型，输出value类型>
 * 可以看Mapper的源码看那些方法需要被实现，那些方法可以被覆盖，覆盖了该方法可以实现什么功能
 */
public class WordCountMapper extends Mapper<LongWritable, Text, Text, IntWritable> {

    private Text k = new Text();
    private IntWritable v = new IntWritable();

    /**
     * map方法会将所有的键值对都调用一次
     */
    @Override
    protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
        // 这里面的key应该是一些可能的id吧

        //mapper过程：获取一行，再分割，然后context写入
        String line = value.toString();
        String[] words = line.split(" ");
        for (String word : words) {
            k.set(word);
            v.set(1);
            context.write(k, v);
        }
    }
}
