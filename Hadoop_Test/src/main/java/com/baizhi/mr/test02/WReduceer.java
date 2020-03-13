package com.baizhi.mr.test02;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;

/*
 *数据是先经由map处理完成后 再交由Reduce 处理
 *此处的keyIn 和valueIn 需要 Map的keyout | valueOut一致|相对应
 * keyOut | valueOut 也是视具体的业务场景而定
 * 根据我们的业务场来说 最终想要输出的类型 （gjf,10）
 * 也就是说 keyOut 为String 类型  valueOut 为Int 类型
 *
 * */
public class WReduceer extends Reducer<Text, IntWritable, Text, IntWritable> {
    @Override
    protected void reduce(Text key, Iterable<IntWritable> values, Context context) throws IOException, InterruptedException {


        /*  key（单词）  values（1的集合）
         * (gjf,[1,1,1,1]) 调用一次reduce 函数
         * (huawei,[1,1,1,1]) 调用一次reduce 函数
         * */


        /*
        * 准备存储总数的字段
        * */
        int sum = 0;


        /*
         * 遍历操作 对于出现的字符串进行累加
         * */

        for (IntWritable value : values) {
            sum += value.get();
        }


        /*
        * 最终数据以 key为单词String类型  value 为总数量 Int 类型输出
        * */
        context.write(key, new IntWritable(sum));

    }
}
