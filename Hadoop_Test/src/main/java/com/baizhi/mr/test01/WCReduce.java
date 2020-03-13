package com.baizhi.mr.test01;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;

public class WCReduce extends Reducer<Text, IntWritable, Text, IntWritable> {
    @Override
    protected void reduce(Text key, Iterable<IntWritable> values, Context context) throws IOException, InterruptedException {
        /*
         *
         * (zhangsan,[1,2,2])
         * */

        int sum = 0;
        for (IntWritable value : values) {
            sum += value.get();
        }

        context.write(key, new IntWritable(sum));

    }
}
