package com.baizhi.mr.test02;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.MRJobConfig;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;

public class WJob {
    public static void main(String[] args) throws Exception {

        System.setProperty("HADOOP_USER_NAME","root");
        /*
         * 准备 Configuration 对象
         * 这里存储着相关配置
         * */
        Configuration configuration = new Configuration();


        configuration.addResource("conf2/core-site.xml");
        configuration.addResource("conf2/hdfs-site.xml");
        configuration.addResource("conf2/mapred-site.xml");
        configuration.addResource("conf2/yarn-site.xml");


        configuration.set("mapreduce.app-submission.cross-platform","true");

        configuration.set(MRJobConfig.JAR,"F:\\3-2大数据\\代码\\BigData\\Hadoop_Test\\target\\Hadoop_Test-1.0-SNAPSHOT.jar");
        /*
         * 准备Job 核心对象
         * 需要填入 Configuration 对象作为参数
         * */
        Job job = Job.getInstance(configuration);

        //job.setJarByClass(WJob.class);


        /*
         * 输入组件 数据就是由组件提供给 Mapper 的
         * */
        job.setInputFormatClass(TextInputFormat.class);

        /*
         * 输出组件 数据就是由该组件输出| 持久化到文件系统中的
         * */

        job.setOutputFormatClass(TextOutputFormat.class);


        /*
         * 设置数据输入路径
         * */
        TextInputFormat.setInputPaths(job, new Path("/word.txt"));
        /*
         * 设置数据输出路径 数据输出路径不可重复
         * */
        TextOutputFormat.setOutputPath(job, new Path("/out3"));



        /*
         * 设置   Job|计算任务的 Mapper 逻辑
         * */
        job.setMapperClass(WMapper.class);

        /*
         * 设置   Job|计算任务的 Reducer 逻辑
         * */

        job.setReducerClass(WReduceer.class);


        /*
         * 设置Mapper端的key的输出类型
         * */
        job.setMapOutputKeyClass(Text.class);
        /*
         * 设置Mapper端的value的输出类型
         * */
        job.setMapOutputValueClass(IntWritable.class);


        /*
         * 设置Reducer端的key的输出类型
         * */
        job.setOutputKeyClass(Text.class);
        /*
         * 设置Reducer端的value的输出类型
         * */
        job.setOutputValueClass(IntWritable.class);

        /*
         * Job
         *提交 两种方式
         * */
        //job.submit();
        job.waitForCompletion(true);


    }
}
