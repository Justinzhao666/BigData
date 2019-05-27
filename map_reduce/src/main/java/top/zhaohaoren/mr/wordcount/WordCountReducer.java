package top.zhaohaoren.mr.wordcount;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;

/**
 * 继承Reducer，具体操作和Mapper相似
 */
public class WordCountReducer extends Reducer<Text, IntWritable, Text, IntWritable> {

    private int count = 0;
    private IntWritable v = new IntWritable();

    /**
     * @param key 这个key是什么东东？干啥的? todo:
     * @param values： 所有的map计算的结果值
     * @param context
     */
    @Override
    protected void reduce(Text key, Iterable<IntWritable> values, Context context) throws IOException, InterruptedException {
        count = 0;
        for (IntWritable value : values) {
            count += value.get();
        }
        v.set(count);
        context.write(key, v);
    }
}
