package com.baizhi.mr.test02;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;

/*
 *
 * 第一步
 * 注意  使用org.apache.hadoop.mapreduce.Mapper; 下的Mapper
 *
 * 第二步
 * keyIn
 * valueInt
 * keyOut
 * valueOut
 *    keyIn 和 valueInt 是数据的输入 ，因为数据不是凭空而来的，是由Hadoop中其它的组件将数据
 *    交由 Mapper 函数进行进行处理
 *
 *    当Mapper 函数处理完成后，会将数据输出，那数据输出就需要定义keyOut keyOut
 *
 *    Map 的作用 ：用来把一组键值(keyIn,valueInt)对映射成(经由Map的计算逻辑)一组新的键值对(keyOut,valueOut)
 *
 *    在这里最重要的是  keyIn valueIn  keyOut valueOut 数据类型定义
 *    在此程序中  keyIn  valueIn 的数据类型是一定，是固定的 不可更改
 *
 * keyIn Long  当前文本的偏移量 LongWritable
 * valueInt String 指的是当前行的文本数据 Text
 *
 * (GJF,1)
 * (GJF,1)
 * keyOut 类型需要更具业务场景而定 String Text
 * valueOut  类型需要更具业务场景而定 Int  IntWritable
 *
 *
 * Hadoop 是集群进行运算 所以在计算中需要通过网络传输 数据
 * 通过网络 传输数据 需要进行序列化和反序列化的操作
 * Hadoop 提供的序列化方案是 Writable
 * 那就是说     不同的数据类型  hadoop 都提供了 Writable 序列化对象
 * String -》Text  只有String 比较特殊
 * Int-》IntWritable
 * Long-》LongWritable
 * Null-》NullWritable
 * .......
 *
 * */
public class WMapper extends Mapper<LongWritable, Text, Text, IntWritable> {
    @Override
    protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
        /*
        Hadoop 会读取我们定义某个文件  将文件中的内容 通过某些手段 给Mapper 函数
        * LongWritable key 当前文本的偏移量 目前此参数对我们无用
        *  Text value  当前行的文本  这个参数才有用 就是拿到了每一行的数据
        * */


        /*
        * 通过 Text 类型的 value 转换过来 拿到每一行的文本值
        * 比如说  这样的 “zhangsan lisi wuwang wuwang”
        * */
        String line = value.toString();


        /*
        * 通过空格进行分割  拿到了当前行的每一行单词
        * */
        String[] words = line.split(" ");



        /*
        * 拿到每一个单词 就可以进行遍历
        * */


        for (String word : words) {

            /*
            * 我们数据输出的格式应该是这样
            * （gjf，1）
            * （gjf，1）
            * 那就是key为string 类型
            * value 为int 类型
            * */

            context.write(new Text(word),new IntWritable(1));
        }

    }

}
