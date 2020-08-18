package com.inforefiner.util;

import com.inforefiner.helper.PropertiesManager;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.*;

import java.io.IOException;
import java.net.URI;
import java.net.URISyntaxException;

public class HDFSClientUtils {

    private static String url = PropertiesManager.getValue("hdfs.url");
    private static String user = PropertiesManager.getValue("hdfs.user");
    private static Configuration configuration = new Configuration();
    static {
        configuration.set("dfs.support.append", "true");
        configuration.set("dfs.client.block.write.replace-datanode-on-failure.policy", "NEVER");
        configuration.set("dfs.client.block.write.replace-datanode-on-failure.enable", "true");
    }


    /**
     * 判断HDFS路径是否存在
     * @param fileHDFS
     * @return
     * @throws URISyntaxException
     * @throws IOException
     * @throws InterruptedException
     */
    public static Boolean existFileHDFS(String fileHDFS) throws URISyntaxException, IOException, InterruptedException {
        Boolean flag = true;
        try{
            if(fileHDFS.startsWith("hdfs")){
                if(!fileHDFS.substring(0, 16).equals(url)){
                    flag = false;
                }
            }
            if(flag != false){
//                FileSystem fileSystem = FileSystem.get(new URI(url), configuration, user);
                FileSystem fileSystem = FileSystem.get(new URI(url), configuration);
                flag = fileSystem.exists(new Path(fileHDFS));
            }
        } catch (Exception e){
            return false;
        }
        return flag;
    }


    /**
     * 获取HDFS路径下所有文件
     * @param fileRoot
     * @return
     * @throws URISyntaxException
     * @throws IOException
     * @throws InterruptedException
     */
    public static RemoteIterator<LocatedFileStatus> getAllFiles(String fileRoot) throws URISyntaxException, IOException, InterruptedException {
        FileSystem fileSystem = FileSystem.get(new URI(url), configuration, user);
        RemoteIterator<LocatedFileStatus> listFiles = fileSystem.listFiles(new Path(fileRoot), true);
        return listFiles;
    }




}
