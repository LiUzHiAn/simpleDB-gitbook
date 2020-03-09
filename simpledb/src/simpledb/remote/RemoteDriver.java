package simpledb.remote;


import java.rmi.Remote;
import java.rmi.RemoteException;

/**
 * @ClassName RemoteDriver
 * @Deacription // TODO
 * @Author LiuZhian
 * @Date 2020-03-09 19:21
 * @Version 1.0
 **/

public interface RemoteDriver extends Remote {
    public RemoteConnection connect() throws RemoteException;
}