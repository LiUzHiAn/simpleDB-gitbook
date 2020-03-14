package simpledb.remote;

import java.rmi.Remote;
import java.rmi.RemoteException;

/**
 * @ClassName RemoteConnection
 * @Deacription // TODO
 * @Author LiuZhian
 * @Date 2020-03-09 19:19
 * @Version 1.0
 **/

public interface RemoteConnection extends Remote {
    public RemoteStatement createStatement() throws RemoteException;

    public void close() throws RemoteException;
}
