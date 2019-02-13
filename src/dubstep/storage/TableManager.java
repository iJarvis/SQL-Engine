package dubstep.storage;

import net.sf.jsqlparser.statement.create.table.CreateTable;

import java.util.HashMap;


public class TableManager {

    HashMap<String, Table> tableDirectory = new HashMap<>();

    public boolean createTable(CreateTable createTable) {
        String tableName = createTable.getTable().getName();
        if (tableDirectory.containsKey(tableName))
            return false;
        else
            tableDirectory.put(tableName, new Table(createTable));
        return true;
    }

    public Table getTable(String tableName) {
        Table table = tableDirectory.get(tableName);
        if (table == null)
            System.out.println("Table not found");
        return table;
    }


}

