package simpledb.scan;

public interface Scan {
    // 移动到第一条记录之前
    public void beforeFirst();

    // 移动到下一条记录，如果没有下一条记录，返回false
    public boolean next();

    // 关闭scan，如果有subScan，也会相应关闭
    public void close();

    // 获取当前记录指定字段的值，被抽象成了一个Constant对象
    public Constant getVal(String fieldName);

    // 获取当前字段指定int字段的值
    public int getInt(String fieldName);

    // 获取当前字段指定string字段的值
    public String getString(String fieldName);

    // 判断是否有指定字段
    public boolean hasField(String fieldName);

}
