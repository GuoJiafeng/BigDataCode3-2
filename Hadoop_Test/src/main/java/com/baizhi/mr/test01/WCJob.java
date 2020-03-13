package com.baizhi.mr.test01;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;


public class WCJob {
    public static void main(String[] args) throws Exception {

        Configuration configuration = new Configuration();

        Job job = Job.getInstance(configuration);

        job.setJarByClass(WCJob.class);

        job.setInputFormatClass(TextInputFormat.class);
        job.setOutputFormatClass(TextOutputFormat.class);


        TextInputFormat.setInputPaths(job, new Path("file:///F:\\3-2大数据\\笔记\\Day03-Hadoop\\数据文件\\word.txt"));
        TextOutputFormat.setOutputPath(job, new Path("file:///F:\\3-2大数据\\笔记\\Day03-Hadoop\\数据文件\\out2"));


        job.setMapperClass(WCMapper.class);
        job.setReducerClass(WCReduce.class);

        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(IntWritable.class);

        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(IntWritable.class);


        job.waitForCompletion(true);
    }
}
