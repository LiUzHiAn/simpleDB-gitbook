package simpledb.remote;

import java.sql.SQLException;

public class SimpleMetaData extends MetaDataAdapter {

    RemoteMetaData rmd;

    public SimpleMetaData(RemoteMetaData rmd) {
        this.rmd = rmd;
    }

    @Override
    public int getColumnCount() throws SQLException {
        try {
            return rmd.getColumnCount();
        } catch (Exception e) {
            throw new SQLException(e);
        }
    }

    @Override
    public String getColumnName(int column) throws SQLException {
        try {
            return rmd.getColumnName(column);
        } catch (Exception e) {
            throw new SQLException(e);
        }
    }

    @Override
    public int getColumnType(int column) throws SQLException {
        try {
            return rmd.getColumnType(column);
        } catch (Exception e) {
            throw new SQLException(e);
        }
    }

    @Override
    public int getColumnDisplaySize(int column) throws SQLException {
        try {
            return rmd.getColumnDisplaySize(column);
        } catch (Exception e) {
            throw new SQLException(e);
        }
    }
}
