package simpledb.index.btree;

import org.omg.CORBA.PUBLIC_MEMBER;
import simpledb.file.Block;
import simpledb.query.Constant;
import simpledb.query.IntConstant;
import simpledb.query.StringConstant;
import simpledb.record.RID;
import simpledb.record.Schema;
import simpledb.record.TableInfo;
import simpledb.tx.Transaction;

import static simpledb.file.Page.BLOCK_SIZE;
import static simpledb.file.Page.INT_SIZE;
import static simpledb.record.Schema.INTEGER;

/**
 * @program simpledb
 * @description: TODO
 * @author: liuzhian
 * @create: 2020/09/23 17:18
 */
public class BTreePage {

    private Block currentBlk;
    private TableInfo tableInfo;
    private Transaction tx;
    private int slotSize;

    public BTreePage(Block currentBlk, TableInfo tableInfo, Transaction tx) {
        this.currentBlk = currentBlk;
        this.tableInfo = tableInfo;
        this.tx = tx;
        this.slotSize = tableInfo.recordLength();
        tx.pin(this.currentBlk);  // 固定块
    }

    public void close() {
        if (currentBlk != null)
            tx.unpin(currentBlk);
        currentBlk = null;
    }

    /**
     * 在当前页中, 找到所有 >= searchKey的records中的第一个。
     * 于是，再调用nextInt()或nextString()就可以得到和searchKey相同的record 的具体取值
     * <p>
     * 和方法 beforeFirst()类似
     *
     * @param searchKey
     * @return
     */
    public int findSlotBefore(Constant searchKey) {
        int slot = 0;
        while (slot < getNumRecords() && getDataVal(slot).compareTo(searchKey) < 0)
            slot++;
        return slot - 1;
    }

    /**
     * 当前块能否再插入一条record
     *
     * @return
     */
    public boolean isFull() {
        return INT_SIZE + INT_SIZE + (getNumRecords() + 1) * slotSize >= BLOCK_SIZE;
    }

    /**
     * 块拆分
     *
     * @param splitPos
     * @param flag
     * @return
     */
    public Block split(int splitPos, int flag) {
        Block newBlk = appendNew(flag);
        BTreePage newPage = new BTreePage(newBlk, tableInfo, tx);
        // 从第splitPos条record开始，把原来block上的内容转移到newBlock上去
        transferRecords(splitPos, newPage);
        newPage.setFlag(flag);  // 标志位保持一致
        newPage.close();
        return newBlk;
    }

    public Block appendNew(int flag) {
        return tx.append(tableInfo.fileName(), new BTreePageFormatter(tableInfo, flag));
    }

    public Constant getDataVal(int slot) {
        return getVal(slot, "dataval"); // 所有索引的字段名就是"dataval"
    }

    /**
     * 获取当前页上的记录数
     *
     * @return
     */
    public int getNumRecords() {
        return tx.getInt(currentBlk, INT_SIZE);  // 每个树页的第4-8个字节，表示的是这个页上现有多少条记录
    }

    /**
     * 获取当前页的标志位
     *
     * @return
     */
    public int getFlag() {
        return tx.getInt(currentBlk, 0);
    }

    /**
     * 修改当前页的标志位
     *
     * @return
     */
    public void setFlag(int newValue) {
        tx.setInt(currentBlk, 0, newValue);
    }

    // ===============只被BTreeDir调用的方法===================
    public int getChild(int slot) {
        return getInt(slot, "block");
    }

    public void insertDir(int slot, Constant val, int blkNum) {
        insert(slot);
        setVal(slot, "dataval", val);
        setInt(slot, "block", blkNum);
    }

    // ===============只被BTreeLeaf调用的方法===================
    public RID getDataRID(int slot) {
        return new RID(getInt(slot, "block"),
                getInt(slot, "id"));
    }

    public void insertLeaf(int slot, Constant val, RID rid) {
        insert(slot);
        setVal(slot, "dataval", val);
        setInt(slot, "block", rid.blockNumber());
        setInt(slot, "id", rid.id());
    }

    /**
     * 删除第slot个record
     * 把slot后面的所有record向前移动1个位置
     *
     * @param slot
     */
    public void delete(int slot) {
        for (int i = slot + 1; i < getNumRecords(); i++) {
            copyRecord(i, i - 1);  // 把第i个record 复制到第i-1个位置
        }
        setNumRecords(getNumRecords() - 1);
    }

    // ===============私有方法===================
    private Constant getVal(int slot, String fieldName) {
        int type = tableInfo.schema().type(fieldName);
        if (type == INTEGER)
            return new IntConstant(getInt(slot, fieldName));
        else
            return new StringConstant(getString(slot, fieldName));
    }

    private void setVal(int slot, String fieldName, Constant newValue) {
        int type = tableInfo.schema().type(fieldName);
        if (type == INTEGER)
            setInt(slot, fieldName, (Integer) newValue.asJavaVal());
        else
            setString(slot, fieldName, (String) newValue.asJavaVal());
    }

    /**
     * 读取第slot个records中，字段fieldName的取值
     *
     * @param slot
     * @param fieldName
     * @return
     */
    private int getInt(int slot, String fieldName) {
        int fldPos = fldPos(slot, fieldName);
        return tx.getInt(currentBlk, fldPos);
    }

    /**
     * 更新 第slot个records中字段fieldName的取值为newValue
     *
     * @param slot
     * @param fieldName
     * @param newValue
     */
    private void setInt(int slot, String fieldName, int newValue) {
        int fldPos = fldPos(slot, fieldName);
        tx.setInt(currentBlk, fldPos, newValue);
    }

    /**
     * 读取第slot个records中，字段fieldName的取值
     *
     * @param slot
     * @param fieldName
     * @return
     */
    private String getString(int slot, String fieldName) {
        int fldPos = fldPos(slot, fieldName);
        return tx.getString(currentBlk, fldPos);
    }

    /**
     * 更新 第slot个records中字段fieldName的取值为newValue
     *
     * @param slot
     * @param fieldName
     * @param newValue
     */
    private void setString(int slot, String fieldName, String newValue) {
        int fldPos = fldPos(slot, fieldName);
        tx.setString(currentBlk, fldPos, newValue);
    }

    /**
     * 得到当前页中，第slot个records中，fieldName字段的position
     *
     * @param slot
     * @param fieldName
     * @return
     */
    private int fldPos(int slot, String fieldName) {
        int fieldOffsetInASingleSlot = tableInfo.offset(fieldName);
        // 第一个INT_SIZE表示当前页中的flag
        // 第二个INT_SIZE表示当前页中的records数
        return INT_SIZE + INT_SIZE + slotSize * slot + fieldOffsetInASingleSlot;
    }

    private void setNumRecords(int num) {
        tx.setInt(currentBlk, INT_SIZE, num);
    }

    /**
     * 将新的记录放置在原来第slot条的位置上
     * <p>
     * 需要将slot后的每条记录都后移一个单位
     * <p>
     * 操作完后，其实还没有具体更新第slot条record的具体取值，需要用户显式地给定
     *
     * @param slot
     */
    private void insert(int slot) {
        for (int i = getNumRecords(); i > slot; i--) {
            copyRecord(i - 1, i);
        }
        setNumRecords(getNumRecords() + 1);
    }

    /**
     * 将第from条record的内容copy到第to条record上去
     *
     * @param from
     * @param to
     */
    private void copyRecord(int from, int to) {
        Schema schema = tableInfo.schema();
        for (String fieldName : schema.fields())
            setVal(to, fieldName, getVal(from, fieldName));
    }

    private void transferRecords(int slot, BTreePage desPage) {
        int destSlot = 0;
        while (slot < getNumRecords()) {
            desPage.insert(destSlot);
            Schema schema = tableInfo.schema();
            for (String fieldName : schema.fields()) {
                desPage.setVal(destSlot, fieldName, getVal(slot, fieldName));
            }
            delete(slot);

            destSlot++;
            slot++;// todo 需要这一句吗
        }
    }
}



