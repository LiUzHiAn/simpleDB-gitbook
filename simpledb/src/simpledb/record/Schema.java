package simpledb.record;


import java.util.Collection;
import java.util.HashMap;
import java.util.Map;


/**
 * @ClassName Schema
 * @Description TODO
 * @Author LiuZhian
 * @Date 2020/2/3 1:09 下午
 * @Version 1.0
 */
public class Schema {
    public static final int INTEGER = 0;
    public static final int VARCHAR = 1;

    private Map<String, FieldInfo> info = new HashMap<>();

    public Schema() {

    }

    public void addField(String fldName, int type, int length) {
        info.put(fldName, new FieldInfo(type, length));
    }

    public void addIntField(String fldName) {
        // int类型的长度设置为0，这里指的是逻辑长度,没什么实际意义，可以设置为任意值
        // 而不是实际物理存储所需字节长度
        addField(fldName, INTEGER, 0);
    }

    public void addStringField(String fldName, int length) {
        addField(fldName, VARCHAR, length);
    }

    /**
     * 从一个已有的schema中添加一个字段到该schema中
     *
     * @param fldName 字段名
     * @param schema  源schema
     */
    public void add(String fldName, Schema schema) {
        int type = schema.type(fldName);
        int length = schema.length(fldName);
        addField(fldName, type, length);
    }

    /**
     * 将已有的schema中的所有字段到该schema中
     *
     * @param schema 源schema
     */
    public void addAll(Schema schema) {
        info.putAll(schema.info);
    }

    public Collection<String> fields() {
        return this.info.keySet();
    }

    public boolean hasFiled(String fldName) {
        return this.info.keySet().contains(fldName);
    }

    public int type(String fldName) {
        return this.info.get(fldName).type;
    }

    public int length(String fldName) {
        return this.info.get(fldName).length;
    }

    private class FieldInfo {
        int type;
        int length;  // 对于string类型的字段，length记录的是可能最长的字符数

        public FieldInfo(int type, int length) {
            this.type = type;
            this.length = length;
        }
    }
}
