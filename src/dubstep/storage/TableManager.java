package dubstep.storage;

import net.sf.jsqlparser.statement.create.table.CreateTable;

import java.util.HashMap;


public class TableManager {

    HashMap<String, DubTable> tableDirectory = new HashMap<>();

    public boolean createTable(CreateTable createTable) {

        String tableName = createTable.getTable().getName();
        if (tableDirectory.containsKey(tableName))
            return false;
        else
            tableDirectory.put(tableName, new DubTable(createTable));
        return true;
    }

    public DubTable getTable(String tableName) {
        DubTable table = tableDirectory.get(tableName);
        if (table == null)
            System.out.println("DubTable not found");
        return table;
    }


}

