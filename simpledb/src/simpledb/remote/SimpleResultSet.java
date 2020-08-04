package simpledb.remote;

import java.sql.ResultSetMetaData;
import java.sql.SQLException;

public class SimpleResultSet extends ResultSetAdapter {
    RemoteResultSet rrs;

    public SimpleResultSet(RemoteResultSet rrs) {
        this.rrs = rrs;
    }

    @Override
    public boolean next() throws SQLException {
        try {
            return rrs.next();
        } catch (Exception e) {
            throw new SQLException(e);
        }
    }

    @Override
    public int getInt(String fieldName) throws SQLException {
        try {
            return rrs.getInt(fieldName);
        } catch (Exception e) {
            throw new SQLException(e);
        }

    }

    @Override
    public String getString(String fieldName) throws SQLException {
        try {
            return rrs.getString(fieldName);
        } catch (Exception e) {
            throw new SQLException(e);
        }
    }

    @Override
    public ResultSetMetaData getMetaData() throws SQLException {
        try {
            RemoteMetaData rmd = rrs.getMetaData();
            return new SimpleMetaData(rmd);
        } catch (Exception e) {
            throw new SQLException(e);
        }
    }

    @Override
    public void close() throws SQLException {
        try {
            rrs.close();
        } catch (Exception e) {
            throw new SQLException(e);
        }
    }
}
