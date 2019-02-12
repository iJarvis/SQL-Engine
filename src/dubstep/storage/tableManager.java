package dubstep.storage;

import net.sf.jsqlparser.statement.create.table.ColumnDefinition;
import net.sf.jsqlparser.statement.create.table.CreateTable;

import java.util.ArrayList;
import java.util.HashMap;


public class tableManager {

    HashMap<String ,table> tableDirectory = new HashMap<String, table>();

    public boolean createTable(CreateTable createTable)
    {
        String tableName = createTable.getTable().getName();
      if(tableDirectory.containsKey(tableName))
            return  false;
        else
            tableDirectory.put(tableName,new table(createTable));
        return  true;
    };



}

class table{

    String tableName;
    ArrayList<ColumnDefinition> columnDefinitions;
    public table(CreateTable createTable)
    {
        tableName = createTable.getTable().getName();
        columnDefinitions = (ArrayList)createTable.getColumnDefinitions();
    };

}
