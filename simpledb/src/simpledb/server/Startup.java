package simpledb.server;

import simpledb.remote.RemoteDriver;
import simpledb.remote.RemoteDriverImpl;

import java.rmi.registry.LocateRegistry;
import java.rmi.registry.Registry;

/**
 * @ClassName Startup
 * @Deacription // TODO
 * @Author LiuZhian
 * @Date 2020-03-14 19:10
 * @Version 1.0
 **/

public class Startup {
    public static void main(String args[]) throws Exception {
        // configure and initialize the database
        SimpleDB.init("lzadb");

        // 在指定端口创建RMI注册表
        Registry reg = LocateRegistry.createRegistry(1099);

        // 服务端访问入口
        RemoteDriver d = new RemoteDriverImpl();
        reg.rebind("simpledb", d);

        System.out.println("database server ready");
    }
}