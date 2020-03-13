package com.baizhi.mr.test03;

import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;

/*
 *
 * */
public class FReducer extends Reducer<Text, FlowBean, NullWritable, FlowBean> {
    @Override
    protected void reduce(Text key, Iterable<FlowBean> values, Context context) throws IOException, InterruptedException {


        /*
         *(13838121211,[{13838121211,31,30,41},.....])
         *
         * */

        /*
         * 准备存储上传流量的字段
         * */
        Long up = 0L;
        /*
         * 准备存储下载流量的字段
         * */
        Long down = 0L;
        /*
         * 准备总流量流量的字段
         * */
        Long sum = 0L;

        for (FlowBean value : values) {

            up += value.getUpFlow();
            down += value.getDownFlow();
            sum += value.getSumFlow();
        }


        FlowBean flowBean = new FlowBean(key.toString(), up, down, sum);

        context.write(NullWritable.get(), flowBean);

    }
}
