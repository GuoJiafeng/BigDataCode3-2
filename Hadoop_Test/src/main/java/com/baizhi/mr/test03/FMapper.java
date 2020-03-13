package com.baizhi.mr.test03;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;

/*
 * 既然想要统计 每个电话的总流量 可以用电话号为keyOut  类型为Text
 *
 * 以Bean对象为valueOut 类型为FlowBean
 * */
public class FMapper extends Mapper<LongWritable, Text, Text, FlowBean> {

    @Override
    protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {

        /*
        * 拿到了 value 即每一行的文本
        * */
        String[] infos = value.toString().split(" ");

        /*
         * 获取下标为0 phone
         * */

        String phone = infos[0];


        /*
         * 获取下标为1 上传流量
         * */


        Long upFlow = Long.valueOf(infos[1]);


        /*
         * 获取下标为2 下载流量
         * */

        Long downFlow = Long.valueOf(infos[2]);


        FlowBean flowBean = new FlowBean(phone, upFlow, downFlow, upFlow + downFlow);

        context.write(new Text(phone), flowBean);


    }
}
