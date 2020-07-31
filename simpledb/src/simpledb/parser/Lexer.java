package simpledb.parser;

import java.io.IOException;
import java.io.StreamTokenizer;
import java.io.StringReader;
import java.util.Arrays;
import java.util.Collection;

/**
 * @ClassName Lexer
 * @Deacription // TODO
 * @Author LiuZhian
 * @Date 2020-02-19 22:47
 * @Version 1.0
 **/

public class Lexer {
    private Collection<String> keywords;
    private StreamTokenizer tokenizer;

    public Lexer(String s) {
        // 初始化关键字
        initKeywords();
        tokenizer = new StreamTokenizer(new StringReader(s));
        // 把'.'也视为一个字符，主要是为了到时候SQL中形如 stuTable.name的字段时，
        // 识别成stuTable,'.'和name三个token，而不是视stuTable.name为一整个token
        tokenizer.ordinaryChar('.');
        // 使关键字和标识符大小写不敏感
        tokenizer.lowerCaseMode(true);
        nextToken();
    }

    // =============判断token类型的相关方法==============

    /**
     * 判断当前token是否是指定的分隔符。
     * <p>
     * 在StreamTokenizer中，单字符token的ttype就是字符对应的ASCII码。
     *
     * @param ch
     * @return
     */
    public boolean matchDelim(char ch) {
        return ch == (char) tokenizer.ttype;
    }

    public boolean matchIntConstant() {
        return tokenizer.ttype == StreamTokenizer.TT_NUMBER;
    }

    /**
     * 匹配String的单引号。
     * <p>
     * 注意，在SimpleDB中，字符串常量用单引号包围。
     *
     * @return
     */
    public boolean matchStringConstant() {
        return '\'' == (char) tokenizer.ttype;
    }

    public boolean matchKeyword(String w) {
        return tokenizer.ttype == StreamTokenizer.TT_WORD &&
                tokenizer.sval.equals(w);
    }

    /**
     * 判断是否是标识符
     * <p>
     * 除了关键字以外的word都视为标识符。
     *
     * @return
     */
    public boolean matchIdentifier() {
        return tokenizer.ttype == StreamTokenizer.TT_WORD &&
                !keywords.contains(tokenizer.sval);
    }

    // =============词法分析器不断“吃掉”当前token的相关方法==============
    public void eatDelim(char ch) {
        if (!matchDelim(ch))
            throw new BadSyntaxException();
        nextToken();
    }

    public int eatIntConstant() {
        if (!matchIntConstant())
            throw new BadSyntaxException();
        int i = (int) tokenizer.nval;
        nextToken();
        return i;
    }

    public String eatStringConstant() {
        if (!matchStringConstant())
            throw new BadSyntaxException();
        String str = tokenizer.sval;
        nextToken();
        return str;
    }

    public void eatKeyword(String w) {
        if (!matchKeyword(w))
            throw new BadSyntaxException();
        nextToken();
    }

    public String eatIdentifier() {
        if (!matchIdentifier())
            throw new BadSyntaxException();
        String str = tokenizer.sval;
        nextToken();
        return str;
    }

    /**
     * 得到下一个token
     */
    private void nextToken() {
        try {
            tokenizer.nextToken();
        } catch (IOException e) {
            throw new BadSyntaxException();
        }
    }

    private void initKeywords() {
        keywords = Arrays.asList("select", "from", "where", "and",
                "insert", "into", "values", "delete",
                "update", "set", "create", "table",
                "varchar", "int", "view", "as", "index", "on");
    }
}