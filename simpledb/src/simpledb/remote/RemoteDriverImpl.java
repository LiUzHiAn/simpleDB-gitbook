package simpledb.remote;

import java.rmi.RemoteException;
import java.rmi.server.UnicastRemoteObject;

/**
 * @ClassName RemoteDriverImpl
 * @Deacription // TODO
 * @Author LiuZhian
 * @Date 2020-03-14 19:53
 * @Version 1.0
 **/

public class RemoteDriverImpl extends UnicastRemoteObject
        implements RemoteDriver {

    public RemoteDriverImpl() throws RemoteException {

    }

    @Override
    public RemoteConnection connect() throws RemoteException {
        return new RemoteConnectionImpl();
    }
}