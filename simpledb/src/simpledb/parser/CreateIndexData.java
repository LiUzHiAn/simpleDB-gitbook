package simpledb.parser;

/**
 * @ClassName CreateIndexData
 * @Deacription // TODO
 * @Author LiuZhian
 * @Date 2020-02-20 21:25
 * @Version 1.0
 **/

public class CreateIndexData {
    private String indexName;
    private String tblName;
    private String fldName;

    public CreateIndexData(String indexName, String tblName,
                           String fldName) {
        this.indexName = indexName;
        this.tblName = tblName;
        this.fldName = fldName;
    }

    public String getIndexName() {
        return indexName;
    }

    public String getTblName() {
        return tblName;
    }

    public String getFldName() {
        return fldName;
    }
}