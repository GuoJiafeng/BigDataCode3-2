package com.baizhi.hdfs;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.*;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.omg.CORBA.PUBLIC_MEMBER;

public class App {
    FileSystem fileSystem;

    Configuration configuration;
    @Before
    public void getClient() throws Exception {


        // System.setProperty("HADOOP_USER_NAME", "root");

         configuration = new Configuration();
        configuration.addResource("core-site.xml");
        configuration.addResource("hdfs-site.xml");


        fileSystem = FileSystem.newInstance(configuration);

    }

    @Test
    public void testUpload() throws Exception {

        /*
         *
         * 从本地拷贝到HDFS 中  两个参数 一个是src 源文件  一个dst 目标文件  类型都是Path
         * */
        /*
         * 本地想要上传到hdfs中的文件
         * */
        Path src = new Path("file:///F:\\3-2大数据\\笔记\\Day03-Hadoop\\数据文件\\1.txt");


        /*
         * hdfs 中文件
         * */
        Path dst = new Path("/3.txt");

        fileSystem.copyFromLocalFile(src, dst);


    }


    @Test
    public void testDownload() throws Exception {

        /*
         *
         * src 就是hdfs 上的文件
         * dst就是本地的文件
         * */


        Path src = new Path("/1.txt");
        Path dst = new Path("file:///F:\\3-2大数据\\笔记\\Day03-Hadoop\\数据文件\\2.txt");
        fileSystem.copyToLocalFile(false, src, dst, true);


    }

    @Test
    public void delete() throws Exception {

        fileSystem.delete(new Path("/1.txt"), true);


    }

    @Test
    public void testMkdir() throws Exception {

        boolean success = fileSystem.mkdirs(new Path("/baizhi1"));

        if (success) {
            System.out.printf("yes");
        } else {

            System.out.printf("no");
        }

    }


    @Test
    public void testList() throws Exception {

        RemoteIterator<LocatedFileStatus> listFiles = fileSystem.listFiles(new Path("/"), true);

        while (listFiles.hasNext()) {

            LocatedFileStatus fileStatus = listFiles.next();

            System.out.println(fileStatus.getPath());

        }


    }

    /*
     * 测试是否存在
     * */
    @Test
    public void testExist() throws Exception {


        boolean exists = fileSystem.exists(new Path("/1.log"));
        if (exists) {
            System.out.printf("yes");
        } else {

            System.out.printf("no");
        }


    }



    @Test
    public void huishouzhan() throws Exception{

        Trash trash = new Trash(fileSystem, configuration);

        boolean b = trash.moveToTrash(new Path("/2.txt"));

        if (b) {
            System.out.printf("yes");
        } else {

            System.out.printf("no");
        }



    }

    @After
    public void close() throws Exception {

        fileSystem.close();

    }
}
