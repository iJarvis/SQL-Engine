package dubstep;

import dubstep.executor.BaseNode;
import dubstep.planner.PlanTree;
import dubstep.storage.TableManager;
import dubstep.utils.QueryTimer;
import dubstep.utils.Tuple;
import net.sf.jsqlparser.parser.CCJSqlParser;
import net.sf.jsqlparser.parser.ParseException;
import net.sf.jsqlparser.statement.Statement;
import net.sf.jsqlparser.statement.create.table.CreateTable;
import net.sf.jsqlparser.statement.select.PlainSelect;
import net.sf.jsqlparser.statement.select.Select;
import net.sf.jsqlparser.statement.select.SelectBody;
import net.sf.jsqlparser.statement.select.Union;

import java.io.StringReader;
import java.sql.SQLException;
import java.util.Scanner;

public class Main {
    public static final String PROMPT = "$>";
    public static TableManager mySchema = new TableManager();
    // Globals used across project
    static public int maxThread = 1;
    static public boolean DEBUG_MODE = false; // will print out logs - all logs should be routed through this flag
    static public boolean EXPLAIN_MODE = true; // will print statistics of the code
    static public int SCAN_BUFER_SIZE = 100; //  number of rows cached per scan from disk

    public static void main(String[] args) throws ParseException, SQLException {
        //Get all command line arguments
        for (int i = 0; i < args.length; i++){
            if (args[i].equals("--in-mem"))
                mySchema.setInMem(true);
            if (args[i].equals("--on-disk"))
                mySchema.setInMem(false);
        }

        Scanner scanner = new Scanner(System.in);
        QueryTimer timer = new QueryTimer();

        System.out.print(PROMPT);
        while (scanner.hasNext()) {

            String sqlString = scanner.nextLine();

            while(sqlString.indexOf(';') < 0)
                sqlString = sqlString + " " + scanner.nextLine();

            if (sqlString == null)
                continue;

            if (sqlString.equals("\\q") || sqlString.equals("quit") || sqlString.equals("exit"))
                break;

            CCJSqlParser parser = new CCJSqlParser(new StringReader(sqlString));
            Statement query = parser.Statement();

            timer.reset();
            timer.start();

            if (query instanceof CreateTable) {
                CreateTable createQuery = (CreateTable) query;
                if (!mySchema.createTable(createQuery)) {
                    System.out.println("Unable to create DubTable - DubTable already exists");
                }
            } else if (query instanceof Select) {
                Select selectQuery = (Select) query;
                SelectBody selectBody = selectQuery.getSelectBody();
                BaseNode root;
                if (selectBody instanceof PlainSelect) {
                    root = PlanTree.generatePlan((PlainSelect) selectBody);
                } else {
                    root = PlanTree.generateUnionPlan((Union) selectBody);
                }
                Tuple tuple = root.getNextTuple();
                while (tuple != null) {
                    System.out.println(tuple.GetProjection());
                    tuple = root.getNextTuple();
                }
                if (EXPLAIN_MODE){
                    Explainer explainer = new Explainer(root);
                    explainer.explain();
                }
            } else {
                throw new java.sql.SQLException("I can't understand " + sqlString);
            }
            timer.stop();
           if(DEBUG_MODE)
            System.out.println("Execution time = " + timer.getTotalTime());
            timer.reset();

            System.out.print(PROMPT);
        }
    }

}
