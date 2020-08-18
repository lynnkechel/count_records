package com.inforefiner.util;



import org.apache.log4j.Logger;
import org.apache.log4j.PropertyConfigurator;

import java.io.File;


/**
 * Properties配置文件处理工具
 *
 * @author cdw
 */
public class LogsUtils {
    // 静态块中不能有非静态属性，所以加static
    private static Logger logger = Logger.getLogger(LogsUtils.class);

    //获取log4j的文件位置
    public static void loadLog4jConf() {
        String log4jPath = System.getProperty("user.dir") + File.separator + "conf" + File.separator + "log4j.properties";
//        String log4jPath = "D:\\Code\\count_records\\src\\main\\resources\\log4j.properties";
        PropertyConfigurator.configure(log4jPath);
        System.setProperty("log4j.configuration", log4jPath);
        logger.info(">> log4jPath" + log4jPath);
    }

}



