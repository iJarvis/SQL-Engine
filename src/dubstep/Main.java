package dubstep;


import net.sf.jsqlparser.parser.CCJSqlParser;
import net.sf.jsqlparser.parser.JSqlParser;
import net.sf.jsqlparser.parser.ParseException;
import net.sf.jsqlparser.schema.Table;
import net.sf.jsqlparser.statement.Statement;
import net.sf.jsqlparser.statement.select.PlainSelect;
import net.sf.jsqlparser.statement.select.Select;
import net.sf.jsqlparser.statement.select.SelectBody;
import sun.tools.jconsole.Tab;

import javax.management.Query;
import java.io.*;
import java.util.Scanner;

public class Main {

    public static void main(String[] args) throws ParseException, IOException {
        Scanner scanner = new Scanner(System.in);


        String expr = scanner.nextLine();
        System.out.println("readed line" + expr);
        CCJSqlParser parser = new CCJSqlParser(new StringReader(expr));
        Statement query = parser.Statement();
        if(query instanceof Select)
        {
            Select select = (Select)query;

            PlainSelect plainSelect =(PlainSelect) select.getSelectBody();
            Table table = (Table) plainSelect.getFromItem();
            String table_name = table.getName();
            BufferedReader br = new BufferedReader(new FileReader("data/"+table_name+".csv"));
            try {
                String line = br.readLine();

                while (line != null) {
                    System.out.println(line);
                    line = br.readLine();
                }
            } catch (IOException e) {
                e.printStackTrace();
            } finally {
                br.close();
            }
        }
        else
        {
            System.out.println("Unknown statement");
        }


    }

}
