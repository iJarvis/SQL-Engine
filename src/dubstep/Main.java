package dubstep;

import dubstep.storage.tableManager;
import net.sf.jsqlparser.parser.CCJSqlParser;
import net.sf.jsqlparser.parser.ParseException;
import net.sf.jsqlparser.statement.Statement;
import net.sf.jsqlparser.statement.create.table.CreateTable;
import net.sf.jsqlparser.statement.select.Select;
import java.sql.SQLException;

import java.io.*;
import java.util.Scanner;

public class Main {
    static tableManager mySchema = new tableManager();

    public static void main(String[] args) throws ParseException, SQLException {
        Scanner scanner = new Scanner(System.in);

        while (scanner.hasNext()) {
            String sql_string = scanner.nextLine();

            if(sql_string == null)
                continue;

            if(sql_string.equals( "\\q") || sql_string.equals("quit") || sql_string.equals("exit"))
                break;


            CCJSqlParser parser = new CCJSqlParser(new StringReader(sql_string));
            Statement query = parser.Statement();

            if (query instanceof CreateTable)
            {
                CreateTable createQuery = (CreateTable) query;
                if (!mySchema.createTable(createQuery))
                    System.out.println("Unable to create table - table already exists");
            }

            else if(query instanceof  Select)
            {
                Select selectQuery = (Select)query;
                System.out.println("select yet to be implemented");
            }

            else
            {
                throw new java.sql.SQLException("I can't understand " + sql_string);
            }

        }
    }

}
