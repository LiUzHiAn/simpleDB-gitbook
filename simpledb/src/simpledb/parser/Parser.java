package simpledb.parser;

import simpledb.query.*;
import simpledb.record.Schema;

import java.util.ArrayList;
import java.util.Collection;

/**
 * @ClassName Parser
 * @Deacription // TODO
 * @Author LiuZhian
 * @Date 2020-02-20 18:54
 * @Version 1.0
 **/

// 语法解析器类
public class Parser {
    private Lexer lexer;

    public Parser(Lexer lexer) {
        this.lexer = lexer;
    }

    //===============解析谓词、项、表达式相关的方法==============
    public String field() {
        return lexer.eatIdentifier();
    }

    public Constant constant() {
        if (lexer.matchIntConstant()) {
            int intVal = lexer.eatIntConstant();
            return new IntConstant(intVal);
        } else {
            String strVal = lexer.eatStringConstant();
            return new StringConstant(strVal);
        }
    }

    public Expression expression() {
        if (lexer.matchIdentifier()) {
            String fieldExtracted = field();
            return new FieldNameExpression(fieldExtracted);
        } else {
            Constant constExtracted = constant();
            return new ConstantExpression(constExtracted);
        }
    }

    public Term term() {
        Expression lhs = expression();
        lexer.eatDelim('=');
        Expression rhs = expression();
        return new Term(lhs, rhs);
    }

    public Predicate predicate() {
        Term t = term();
        Predicate pred = new Predicate(t);

        if (lexer.matchKeyword("and")) {
            lexer.eatKeyword("and");
            pred.conjoinWith(predicate());
        }
        return pred;
    }

    //===============解析Query相关的方法==============
    private Collection<String> tableList() {
        Collection<String> list = new ArrayList<>();
        // 最少有一个表名
        list.add(lexer.eatIdentifier());
        if (lexer.matchDelim(',')) {
            lexer.eatDelim(',');
            list.addAll(tableList()); // 递归调用
        }
        return list;
    }

    private Collection<String> selectList() {
        Collection<String> list = new ArrayList<>();
        // 最少有一个字段名
        list.add(lexer.eatIdentifier());
        if (lexer.matchDelim(',')) {
            lexer.eatDelim(',');
            list.addAll(selectList()); // 递归调用
        }
        return list;
    }

    public QueryData query() {
        lexer.eatKeyword("select");
        Collection<String> fields = selectList();
        lexer.eatKeyword("from");
        Collection<String> tables = tableList();
        // 如果有谓词
        Predicate pred = new Predicate();
        if (lexer.matchKeyword("where")) {
            lexer.eatKeyword("where");
            pred = predicate();
        }

        return new QueryData(fields, tables, pred);
    }

    //===============解析各种update命令相关的方法==============
    public Object updateCmd() {
        if (lexer.matchKeyword("insert"))
            return insert();
        else if (lexer.matchKeyword("delete"))
            return delete();
        else if (lexer.matchKeyword("update"))
            return modify();
        else
            return create();
    }

    private Object create() {
        lexer.eatKeyword("create");
        if (lexer.matchKeyword("table"))
            return createTable();
        else if (lexer.matchKeyword("view"))
            return createView();
        else
            return createIndex();
    }

    //===============解析insert相关的方法==============
    public InsertData insert() {
        lexer.eatKeyword("insert");
        lexer.eatKeyword("into");

        String tblName = lexer.eatIdentifier();
        lexer.eatDelim('(');
        Collection<String> fields = fieldList();
        lexer.eatDelim(')');

        lexer.eatKeyword("values");
        lexer.eatDelim('(');
        Collection<Constant> vals = constList();
        lexer.eatDelim(')');

        return new InsertData(tblName, fields, vals);
    }

    private Collection<String> fieldList() {
        Collection<String> list = new ArrayList<>();
        list.add(lexer.eatIdentifier());
        if (lexer.matchDelim(',')) {
            lexer.eatDelim(',');
            list.addAll(fieldList());

        }
        return list;
    }

    private Collection<Constant> constList() {
        Collection<Constant> list = new ArrayList<>();
        list.add(constant());
        if (lexer.matchDelim(',')) {
            lexer.eatDelim(',');
            list.addAll(constList());
        }
        return list;
    }

    //===============解析delete相关的方法==============
    public DeleteData delete() {
        lexer.eatKeyword("delete");
        lexer.eatKeyword("from");
        String tblName = lexer.eatIdentifier();
        Predicate pred = new Predicate();
        // 如果存在where语句
        if (lexer.matchKeyword("where")) {
            lexer.eatKeyword("where");
            pred = predicate();
        }
        return new DeleteData(tblName, pred);
    }

    //===============解析modify相关的方法==============
    public ModifyData modify() {
        lexer.eatKeyword("update");
        String tblName = lexer.eatIdentifier();

        lexer.eatKeyword("set");
        String fldName = field();

        lexer.eatDelim('=');
        Expression newVal = expression();

        Predicate pred = new Predicate();
        // 如果存在where语句
        if (lexer.matchKeyword("where")) {
            lexer.eatKeyword("where");
            pred = predicate();
        }
        return new ModifyData(tblName, fldName, newVal, pred);
    }

    //===============解析CreateTable相关的方法==============
    public CreateTableData createTable() {
        lexer.eatKeyword("table");
        String tblName = lexer.eatIdentifier();
        lexer.eatDelim('(');
        Schema schema = fieldDefs();
        lexer.eatDelim(')');

        return new CreateTableData(tblName, schema);
    }

    private Schema fieldDefs() {
        Schema schema = fieldDef();
        if (lexer.matchDelim(',')) {
            lexer.eatDelim(',');
            schema.addAll(fieldDefs());
        }
        return schema;
    }

    private Schema fieldDef() {
        String fldName = lexer.eatIdentifier();
        return typeDef(fldName);
    }

    private Schema typeDef(String fldName) {
        Schema schema = new Schema();
        if (lexer.matchKeyword("int")) {
            lexer.eatKeyword("int");
            schema.addIntField(fldName);
        } else {
            lexer.eatKeyword("varchar");
            lexer.eatDelim('(');
            int strLen = lexer.eatIntConstant();
            lexer.eatDelim(')');
            schema.addStringField(fldName, strLen);
        }
        return schema;
    }

    //===============解析CreateView相关的方法==============
    public CreateViewData createView() {
        lexer.eatKeyword("view");
        String viewName = lexer.eatIdentifier();
        lexer.eatKeyword("as");
        QueryData qd = query();

        return new CreateViewData(viewName, qd);
    }

    //===============解析CreateIndex相关的方法==============
    public CreateIndexData createIndex() {
        lexer.eatKeyword("index");
        String indexName = lexer.eatIdentifier();
        lexer.eatKeyword("on");
        String tblName = lexer.eatIdentifier();
        lexer.eatDelim('(');
        String fldName = field();
        lexer.eatDelim(')');

        return new CreateIndexData(indexName, tblName, fldName);
    }
}