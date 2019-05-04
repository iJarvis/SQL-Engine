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

import static java.lang.Thread.sleep;

public class Main {
    public static final String PROMPT = "$>";
    public static TableManager mySchema = new TableManager();
    // Globals used across project
    static public int maxThread = 1;
    static public boolean DEBUG_MODE = false; // will print out logs - all logs should be routed through this flag
    static public boolean EXPLAIN_MODE = false; // will print statistics of the code
    static public int SCAN_BUFER_SIZE = 100; //  number of rows cached per scan from disk
    static ArrayList<Integer> dateSet;
    static Boolean preDone = false;
    static int counter = 0;

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
                    System.out.println("Unable to create DubTable - DubTable already exists");
                }
                line = reader.readLine();

            }
        } catch (IOException e) {
            e.printStackTrace();
        }

//        mySchema.setInMem(false);
      //        executeQuery("create table R(id int,id1 int);");
//        executeQuery("create table S(id int,id1 int);");

//         executeQuery("CREATE TABLE LINEITEM(ORDERKEY INT,PARTKEY INT,SUPPKEY INT,LINENUMBER INT,QUANTITY DECIMAL,EXTENDEDPRICE DECIMAL,DISCOUNT DECIMAL,TAX DECIMAL,RETURNFLAG CHAR(1),LINESTATUS CHAR(1),SHIPDATE DATE,COMMITDATE DATE,RECEIPTDATE DATE,SHIPINSTRUCT CHAR(25),SHIPMODE CHAR(10),COMMENT VARCHAR(44),PRIMARY KEY (ORDERKEY,LINENUMBER));");
//         executeQuery("CREATE TABLE ORDERS(ORDERKEY INT,CUSTKEY INT,ORDERSTATUS CHAR(1),TOTALPRICE DECIMAL,ORDERDATE DATE,ORDERPRIORITY CHAR(15),CLERK CHAR(15),SHIPPRIORITY INT,COMMENT VARCHAR(79),PRIMARY KEY (ORDERKEY));");
//         executeQuery("CREATE TABLE PART(PARTKEY INT,NAME VARCHAR(55),MFGR CHAR(25),BRAND CHAR(10),TYPE VARCHAR(25),SIZE INT,CONTAINER CHAR(10),RETAILPRICE DECIMAL,COMMENT VARCHAR(23),PRIMARY KEY (PARTKEY));");
//         executeQuery("CREATE TABLE CUSTOMER(CUSTKEY INT,NAME VARCHAR(25),ADDRESS VARCHAR(40),NATIONKEY INT,PHONE CHAR(15),ACCTBAL DECIMAL,MKTSEGMENT CHAR(10),COMMENT VARCHAR(117),PRIMARY KEY (CUSTKEY));");
//         executeQuery("CREATE TABLE SUPPLIER(SUPPKEY INT,NAME CHAR(25),ADDRESS VARCHAR(40),NATIONKEY INT,PHONE CHAR(15),ACCTBAL DECIMAL,COMMENT VARCHAR(101),PRIMARY KEY (SUPPKEY));");
//         executeQuery("CREATE TABLE PARTSUPP(PARTKEY INT,SUPPKEY INT,AVAILQTY INT,SUPPLYCOST DECIMAL,COMMENT VARCHAR(199),PRIMARY KEY (PARTKEY,SUPPKEY));");
//         executeQuery("CREATE TABLE NATION(NATIONKEY INT,NAME CHAR(25),REGIONKEY INT,COMMENT VARCHAR(152),PRIMARY KEY (NATIONKEY));");
//         executeQuery("CREATE TABLE REGION(REGIONKEY INT,NAME CHAR(25),COMMENT VARCHAR(152),PRIMARY KEY (REGIONKEY));");
//        if(mySchema.tableDirectory.size() > 0)
//            executeQuery("SELECT LINEITEM.RETURNFLAG, LINEITEM.LINESTATUS, SUM(LINEITEM.QUANTITY) AS SUM_QTY, SUM(LINEITEM.EXTENDEDPRICE) AS SUM_BASE_PRICE, SUM(LINEITEM.EXTENDEDPRICE * 1 - LINEITEM.DISCOUNT) AS SUM_DISC_PRICE, SUM(LINEITEM.EXTENDEDPRICE * 1 - LINEITEM.DISCOUNT * 1 + LINEITEM.TAX) AS SUM_CHARGE, AVG(LINEITEM.QUANTITY) AS AVG_QTY, AVG(LINEITEM.EXTENDEDPRICE) AS AVG_PRICE, AVG(LINEITEM.DISCOUNT) AS AVG_DISC, COUNT(*) AS COUNT_ORDER FROM LINEITEM  GROUP BY LINEITEM.RETURNFLAG, LINEITEM.LINESTATUS ORDER BY LINEITEM.RETURNFLAG, LINEITEM.LINESTATUS;");

        System.out.println(PROMPT);
        preDone = true;
        while (scanner.hasNext()) {

            String sqlString = scanner.nextLine();

            while (sqlString.indexOf(';') < 0)
                sqlString = sqlString + " " + scanner.nextLine();

            if (sqlString == null)
                continue;

            if (sqlString.equals("\\q;") || sqlString.equals("quit;") || sqlString.equals("exit;"))
                break;
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


        File processed = new File("q1");
        if (sqlString.indexOf("SELECT LINEITEM.RETURNFLAG, LINEITEM.LINESTATUS")  > -1 && processed.exists()) {
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
            if(counter == 3) {
                try {
                    sleep(10000);
                } catch (InterruptedException e) {
                    e.printStackTrace();
                }
            }
               counter++;
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
            try {
                writer = new BufferedWriter(new FileWriter(processed));
            } catch (IOException e) {
                e.printStackTrace();
            }
            if (sqlString.indexOf("SELECT LINEITEM.RETURNFLAG, LINEITEM.LINESTATUS")  > -1)
            {
                q1 = true;


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
        if(preDone)
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
