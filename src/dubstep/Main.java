package dubstep;

import com.sun.org.apache.xpath.internal.operations.Bool;
import dubstep.storage.table;
import dubstep.storage.tableManager;
import dubstep.utils.queryTimer;
import dubstep.utils.tuple;
import net.sf.jsqlparser.parser.CCJSqlParser;
import net.sf.jsqlparser.parser.ParseException;
import net.sf.jsqlparser.statement.Statement;
import net.sf.jsqlparser.statement.create.table.CreateTable;
import net.sf.jsqlparser.statement.select.PlainSelect;
import net.sf.jsqlparser.statement.select.Select;

import java.io.StringReader;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.Scanner;

public class Main {
    static tableManager mySchema = new tableManager();

    // Globals used across project
    static public Integer maxThread = 1;
    static public Boolean debug_mode = false; // will print out logs - all logs should be routed through this flag
    static public Boolean explain_mode = false; // will print statistics of the code
    static public Integer scanBufferSize = 100; //  number of rows cached per scan from disk

    public static void main(String[] args) throws ParseException, SQLException {
        Scanner scanner = new Scanner(System.in);
        queryTimer timer = new queryTimer();

        while (scanner.hasNext()) {

            String sql_string = scanner.nextLine();

            if (sql_string == null)
                continue;

            if (sql_string.equals("\\q") || sql_string.equals("quit") || sql_string.equals("exit"))
                break;


            CCJSqlParser parser = new CCJSqlParser(new StringReader(sql_string));
            Statement query = parser.Statement();

            timer.reset();
            timer.start();

            if (query instanceof CreateTable) {
                CreateTable createQuery = (CreateTable) query;
                if (!mySchema.createTable(createQuery))
                    System.out.println("Unable to create table - table already exists");
            } else if (query instanceof Select) {
                Select selectQuery = (Select) query;
                PlainSelect plainSelect = (PlainSelect) selectQuery.getSelectBody();
                String tableName = plainSelect.getFromItem().toString();
                table table = mySchema.getTable(tableName);
                if (table == null)
                    continue;
                table.initRead();
                ArrayList<tuple> tupleBuffer = new ArrayList<tuple>();
                table.readTuples(20, tupleBuffer);
                for (tuple tuple : tupleBuffer) {
                    System.out.println(tuple.getProjection());
                }


                System.out.println("select yet to be implemented");
            } else {
                throw new java.sql.SQLException("I can't understand " + sql_string);
            }
            timer.stop();
            System.out.println("Execution time = " + timer.getTotalTime());
            timer.reset();


        }
    }

}
