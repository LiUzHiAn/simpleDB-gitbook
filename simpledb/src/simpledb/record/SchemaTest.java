package simpledb.record;

public class SchemaTest {
    public static void main(String[] args) {
        Schema sch= new Schema();
        sch.addIntField("cid");
        sch.addStringField("title", 20);
        sch.addIntField("deptid");
        TableInfo ti = new TableInfo("course", sch);

        for (String fldname : ti.schema().fields()) {
            int offset = ti.offset(fldname);
            System.out.println(fldname + " has offset " + offset);
        }
    }
}
