package simpledb.record;

import simpledb.file.Block;
import simpledb.tx.Transaction;

import static simpledb.file.Page.*;

/**
 * @ClassName RecordPage
 * @Description TODO
 * @Author LiuZhian
 * @Date 2020/2/3 2:42 下午
 * @Version 1.0
 */
public class RecordPage {
    public static final int EMPTY = 0, INUSE = 1;

    private Block blk;
    private TableInfo tableInfo;
    private Transaction tx;
    private int slotSize;
    private int currentSlot = -1;

    public RecordPage(Block blk, TableInfo tableInfo, Transaction tx) {
        this.blk = blk;
        this.tableInfo = tableInfo;
        this.tx = tx;
        tx.pin(blk);    // 固定页
        slotSize = tableInfo.recordLength() + INT_SIZE; // 记录长度 + 标志字节
    }


    public int getInt(String fieldName) {
        int fieldPos = fieldPos(fieldName);
        return tx.getInt(blk, fieldPos);  // 会获得块粒度的共享锁
    }

    public String getString(String fieldName) {
        int fieldPos = fieldPos(fieldName);
        return tx.getString(blk, fieldPos);  // 会获得块粒度的共享锁
    }

    public void setInt(String fieldName, int newVal) {
        int fieldPos = fieldPos(fieldName);
        tx.setInt(blk, fieldPos, newVal);  // 会获得块粒度的互斥锁
    }

    public void setString(String fieldName, String newVal) {
        int fieldPos = fieldPos(fieldName);
        tx.setString(blk, fieldPos, newVal);
    }


    public boolean next() {
        return searchFor(INUSE);
    }

    public void delete() {
        int flagPos = currentPos();
        tx.setInt(blk, flagPos, EMPTY);
        // 被删除的记录仍保留在当前记录中，
        // 客户端代码必须显式地调用next()方法来移动至下一条记录。
    }

    /**
     * 主动释放已经固定的块，这个方法主要是给RecordFile类对象
     * moveTo()时调用，主动unpin掉已经固定的块。
     */
    public void close() {
        if (blk != null)
            tx.unpin(blk);
        blk = null;
    }

    /**
     * 插入一条新的记录。
     * <p>
     * 首先会从头开始找到一个空的槽。
     * 1. 如果存在，则将该槽的标志字节设置为INUSE，且currentSlot为插入的槽
     * 2. 如果不存在，返回false
     *
     * @return
     */
    public boolean insert() {
        currentSlot = -1;
        // 找到第一个空的槽
        boolean found = searchFor(EMPTY);
        if (found) {
            int foundSlotStartPos = currentPos();
            tx.setInt(blk, foundSlotStartPos, INUSE);
            // 这里只是将标志位进行了设置，并没有填充插入的记录的具体值
        }
        return found;
    }

    /**
     * 移动到指定ID的槽
     *
     * @param id
     */
    public void moveToID(int id) {
        currentSlot = id;
    }

    public int currentID() {
        return currentSlot;
    }

    /**
     * 获得当前槽的开始位置
     *
     * @return
     */
    private int currentPos() {
        return currentSlot * slotSize;
    }

    /**
     * 获取指定字段的offset，该计算公式为（RL+ 4）* k + 4 + off
     * <p>
     * 其中，RL为一条记录的长度，4为标志字节的Int长度，k为当前槽号，
     * off为指定字段在一条记录中的偏移量
     *
     * @param fileName
     * @return
     */
    private int fieldPos(String fileName) {
        int off = tableInfo.offset(fileName);
        return currentPos() + INT_SIZE + off;
    }

    /**
     * 检查如果以当前位置为新槽的起始位置，解析一个槽，是否会顶出一个块的大小
     *
     * @return true--不会超出，即有效 false--超出，必然无效
     */
    private boolean isValidSlot() {
        return currentPos() + slotSize <= BLOCK_SIZE;
    }

    /**
     * 找到下一个标志字节为指定值的槽
     *
     * @param flag 标志字节的具体取值
     * @return true--找到了，currentSlot为找到槽的槽号 false--找不到
     */
    private boolean searchFor(int flag) {
        currentSlot++;
        while (isValidSlot()) {
            int stepSlotStartPos = currentPos();
            // 检查标志字节
            if (flag == tx.getInt(blk, stepSlotStartPos))
                return true;
            currentSlot++;
        }
        return false;
    }

}
