package mr.hbase2hbase;

import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.io.ImmutableBytesWritable;
import org.apache.hadoop.hbase.mapreduce.TableReducer;
import org.apache.hadoop.io.NullWritable;

import java.io.IOException;

/**
 * TableReducer<KEYIN, VALUEIN, KEYOUT> extends Reducer<KEYIN, VALUEIN, KEYOUT, Mutation>
 * 默认写好了Mutation 是个抽象类：Ctrl+H 查看其所有的实现：
 * Append (org.apache.hadoop.hbase.client)
 * Delete (org.apache.hadoop.hbase.client)
 * Increment (org.apache.hadoop.hbase.client)
 * Put (org.apache.hadoop.hbase.client) -- 这里是put 所以mapper value out是put类型，这样reducer可以把mapper数据直接返回出去
 */
public class FruitReducer extends TableReducer<ImmutableBytesWritable, Put, NullWritable> {
    @Override
    protected void reduce(ImmutableBytesWritable key, Iterable<Put> values, final Context context) throws IOException, InterruptedException {
        // 遍历写出
        for (Put value : values) {
            context.write(NullWritable.get(), value);
        }
    }
}
