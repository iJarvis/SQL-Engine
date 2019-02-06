package dubstep;


import net.sf.jsqlparser.JSQLParserException;
import net.sf.jsqlparser.parser.CCJSqlParserUtil;
import net.sf.jsqlparser.statement.Statement;

public class Main {

    Statement stmt;

    {
        try {
            stmt = CCJSqlParserUtil.parse("SELECT * FROM tab1");

        } catch (JSQLParserException e) {
            e.printStackTrace();
        }

        if(stmt.)
    }

}
