package dubstep;


import net.sf.jsqlparser.parser.CCJSqlParser;
import net.sf.jsqlparser.parser.ParseException;
import net.sf.jsqlparser.schema.Table;
import net.sf.jsqlparser.statement.Statement;
import net.sf.jsqlparser.statement.select.PlainSelect;
import net.sf.jsqlparser.statement.select.Select;
import java.sql.SQLException;

import java.io.*;
import java.util.Scanner;

public class Main {

    public static void main(String[] args) throws ParseException, IOException, SQLException {
        Scanner scanner = new Scanner(System.in);


        String expr = scanner.nextLine();
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
            throw new java.sql.SQLException("I can't understand "+query);
        }


    }

}
