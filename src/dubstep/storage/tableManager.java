package dubstep.storage;

import net.sf.jsqlparser.statement.create.table.CreateTable;

import java.util.HashMap;


public class tableManager {

    HashMap<String, table> tableDirectory = new HashMap<>();

    public boolean createTable(CreateTable createTable) {
        String tableName = createTable.getTable().getName();
        if (tableDirectory.containsKey(tableName))
            return false;
        else
            tableDirectory.put(tableName, new table(createTable));
        return true;
    }

    public table getTable(String tableName) {
        table table = tableDirectory.get(tableName);
        if (table == null)
            System.out.println("table not found");
        return table;
    }


}

