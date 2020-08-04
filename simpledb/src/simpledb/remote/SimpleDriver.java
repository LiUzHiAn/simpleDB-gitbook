package simpledb.remote;

import java.rmi.Naming;
import java.sql.Connection;
import java.sql.Driver;
import java.sql.SQLException;
import java.util.Properties;

public class SimpleDriver extends DriverAdapter {

    /**
     * 客户端根据指定的url访问数据库，得到一个连接。
     * 首先，我们需要将JDBC格式的url换成RMI格式；
     * 然后，我们通过Naming绑定机制，在RMI注册表中得到RemoteDriver对象的存根；
     * 最后，通过调用RemoteDriver存根对象的相关方法来获取RemoteConnection对象的存根。
     *
     * @param url
     * @param info
     * @return
     * @throws SQLException
     */
    @Override
    public Connection connect(String url, Properties info) throws SQLException {
        try {
            // 把JDBC格式的url换成RMI格式
            String newURL = url.replace("jdbc:simpledb", "rmi") + "/simpledb";
            RemoteDriver rdvr = (RemoteDriver) Naming.lookup(newURL);
            RemoteConnection rconn = rdvr.connect();

            return new SimpleConnection(rconn);
        } catch (Exception e) {
            throw new SQLException(e);
        }
    }
}
