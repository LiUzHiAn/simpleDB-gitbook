package simpledb.server;

import simpledb.buffer.BufferMgr;
import simpledb.file.Block;
import simpledb.file.FileMgr;
import simpledb.file.Page;
import simpledb.log.LogMgr;

import java.io.IOException;

/**
 * @ClassName SimpleDB
 * @Deacription // TODO
 * @Author LiuZhian
 * @Date 2020-01-14 15:20
 * @Version 1.0
 **/

public class SimpleDB {

    private static FileMgr fm;
    private static LogMgr lm;
    private static BufferMgr bm;

    /**
     * 初始化数据库系统
     * @param dirName 数据库保存的目录名
     */
    public static void init(String dirName) {
        // 初始化文件管理器
        initFileMgr(dirName);
        // 初始化日志管理器  TODO
        // initLogMgr("testLog.log");
        // 初始化缓冲管理器
        initBufferMgr(10);

        boolean isNew = fm.isNew();
        if (isNew) {
            System.out.println("creating a new database");
        } else {
            System.out.println("recovering the existing database");
        }
    }

    /**
     * 初始化文件管理器
     * @param dirname 数据库文件夹名
     */
    private static void initFileMgr(String dirname) {
        fm = new FileMgr(dirname);
    }
    /**
     * 初始化日志管理器
     * @param logFileName 日志文件名
     */
    private static void initLogMgr(String logFileName) {
        try {
            lm = new LogMgr(logFileName);
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    /**
     * 初始化缓冲管理器
     * @param bufferNums
     */
    private static void initBufferMgr(int bufferNums)
    {
        bm=new BufferMgr(bufferNums);
    }

    public static FileMgr fileMgr() {
        return fm;
    }
    public static LogMgr logMgr() {
        return lm;
    }
    public static BufferMgr bufferMgr(){return  bm;}
}