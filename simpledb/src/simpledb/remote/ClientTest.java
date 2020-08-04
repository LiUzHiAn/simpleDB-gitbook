package simpledb.remote;

import java.rmi.RemoteException;
import java.rmi.registry.LocateRegistry;
import java.rmi.registry.Registry;
import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;

public class ClientTest {
    public static void main(String[] args) throws RemoteException {
        // ============查看本机的rmi registry================
//        String host = "localhost";
//        int port = 1099;
//        Registry registry = LocateRegistry.getRegistry(host, port);
//        for (String name : registry.list()) {
//            System.out.println(name);
//        }
        // ============查看本机的rmi registry================

        SimpleDriver driver = new SimpleDriver();
        try {
            Connection conn = driver.connect("jdbc:simpledb://localhost:1099", null);

            Statement stmt = conn.createStatement();

            String qryStr = "select sid,sname,age from student";
            ResultSet rs = stmt.executeQuery(qryStr);

            while (rs.next()) {
                System.out.println(rs.getInt("sid") + " " +
                        rs.getString("sname") + " " +
                        rs.getInt("age"));
            }
            rs.close();

        } catch (SQLException throwables) {
            throwables.printStackTrace();
        }
    }

}
