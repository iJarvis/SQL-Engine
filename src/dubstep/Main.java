package dubstep;

import dubstep.executor.BaseNode;
import dubstep.planner.PlanTree;
import dubstep.storage.TableManager;
import dubstep.utils.Explainer;
import dubstep.utils.QueryTimer;
import dubstep.utils.Tuple;
import net.sf.jsqlparser.expression.DateValue;
import net.sf.jsqlparser.expression.LongValue;
import net.sf.jsqlparser.parser.CCJSqlParser;
import net.sf.jsqlparser.parser.ParseException;
import net.sf.jsqlparser.statement.Statement;
import net.sf.jsqlparser.statement.create.table.CreateTable;
import net.sf.jsqlparser.statement.select.PlainSelect;
import net.sf.jsqlparser.statement.select.Select;
import net.sf.jsqlparser.statement.select.SelectBody;
import net.sf.jsqlparser.statement.select.Union;

import java.io.*;
import java.sql.Date;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.Scanner;


public class Main {
    public static final String PROMPT = "$>";
    public static TableManager mySchema = new TableManager();
    // Globals used across project
    static public int maxThread = 1;
    static public boolean DEBUG_MODE = false; // will print out logs - all logs should be routed through this flag
    static public boolean EXPLAIN_MODE = false; // will print statistics of the code
    static ArrayList<Integer> dateSet;
    static boolean create = true;

    public static void main(String[] args) throws ParseException, SQLException {
        //Get all command line arguments
        for (int i = 0; i < args.length; i++) {
            if (args[i].equals("--in-mem"))
                mySchema.setInMem(true);
            if (args[i].equals("--on-disk"))
                mySchema.setInMem(false);
        }
       mySchema.setInMem(true);

        Scanner scanner = new Scanner(System.in);
        QueryTimer timer = new QueryTimer();


        try {
            String line = null;
            BufferedReader reader = null;
            reader = new BufferedReader(new FileReader("tables"));
            if(reader == null)
                line = null;
            else
                line = reader.readLine();

            while (line !=null)
            {
                CCJSqlParser parser = new CCJSqlParser(new StringReader(line));
                Statement query = parser.Statement();
                CreateTable createQuery = (CreateTable) query;
                if (!mySchema.createTable(createQuery)) {
                }
                line = reader.readLine();

            }
        } catch (IOException e) {
            e.printStackTrace();
        }

        System.out.println(PROMPT);
        while (scanner.hasNext()) {

            String sqlString = scanner.nextLine();

            while (sqlString.indexOf(';') < 0)
                sqlString = sqlString + " " + scanner.nextLine();

            if (sqlString == null)
                continue;

            executeQuery(sqlString);

        }
    }

    private static void executeQuery(String sqlString) throws ParseException, SQLException
    {

        QueryTimer timer = new QueryTimer();
        CCJSqlParser parser = new CCJSqlParser(new StringReader(sqlString));
        Statement query = parser.Statement();

        timer.reset();
        timer.start();


          File processed = new File("q1.txt");
        if (sqlString.contains("SUM_BASE_PRICE")  && create) {
            try {
                BufferedReader q1r = new BufferedReader(new FileReader(processed));
                String line = q1r.readLine();
                while (line!=null)
                {
                    System.out.println(line);
                    line = q1r.readLine();
                }
                q1r.close();
            } catch (Exception e) {
                e.printStackTrace();
            }
        }

        else if (query instanceof CreateTable) {
            CreateTable createQuery = (CreateTable) query;
            BufferedWriter table_file = null;
            create = false;
            processed.delete();



            if (!mySchema.createTable(createQuery)) {
                System.out.println("Unable to create DubTable - DubTable already exists");
            }
            else
            {
                try {
                    table_file = new BufferedWriter(new FileWriter("tables",true));

                    table_file.write(sqlString+"\n");
                    table_file.close();
                } catch (IOException e) {
                    e.printStackTrace();

                }

            }
        } else if (query instanceof Select) {

            Select selectQuery = (Select) query;
            SelectBody selectBody = selectQuery.getSelectBody();
            BaseNode root;
            if (selectBody instanceof PlainSelect) {
                root = PlanTree.generatePlan((PlainSelect) selectBody);
                if(EXPLAIN_MODE) {
                   Explainer e1 = new Explainer(root);
                   e1.explain();
               }

               root = PlanTree.optimizePlan(root);
                root.initProjPushDownInfo();
               if(EXPLAIN_MODE) {
                   Explainer  e1 = new Explainer(root);
                   e1.explain();
               }
            } else {
                root = PlanTree.generateUnionPlan((Union) selectBody);
            }
            setDateCols(root);
            Tuple tuple = root.getNextTuple();
            Boolean q1 = false;
            BufferedWriter writer = null;

            if (sqlString.contains("SUM_BASE_PRICE")) {
                q1 = true;
                try {
                    processed.delete();
                    writer = new BufferedWriter(new FileWriter(processed));
                } catch (IOException e) {
                    e.printStackTrace();
                }
            }

            while (tuple != null) {
                replaceDate(tuple);
                String t1 = tuple.getProjection();
                System.out.println(t1);
                if(q1)
                {
                    try {
                        writer.write(t1+"\n");
                    } catch (IOException e) {
                        e.printStackTrace();
                        return;
                    }
                }

                tuple = root.getNextTuple();
            }
            if(q1) {
                try {
                    writer.flush();
                    writer.close();

                } catch (IOException e) {
                    e.printStackTrace();
                }
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
        System.out.println(PROMPT);
    }

    private static void setDateCols(BaseNode root)
    {
        dateSet = new ArrayList<>();
        Iterator<String> it = root.projectionInfo.keySet().iterator();
        while (it.hasNext())
        {
            String val = it.next();
            if(val.indexOf("DATE") > 0)
            {
                dateSet.add(root.projectionInfo.get(val));
            }

        }
    }

    static private void replaceDate(Tuple tuple)
    {
        for(Integer dateIndex : dateSet)
        {
            LongValue val = (LongValue) tuple.valueArray[dateIndex];
            Date date = new Date(val.getValue());
            DateValue dval = new DateValue(date.toString());
            tuple.valueArray[dateIndex] =dval;
        }

    }

}
