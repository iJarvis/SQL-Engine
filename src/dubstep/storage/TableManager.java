package dubstep.storage;

import net.sf.jsqlparser.schema.Table;
import net.sf.jsqlparser.statement.create.table.CreateTable;
import net.sf.jsqlparser.statement.select.FromItem;

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
        if (table == null) {
            throw new IllegalStateException("Table " + tableName + " not found");
        }
        return table;
    }

    public void checkTableExists(FromItem fromItem) {
        if (!(fromItem instanceof Table)) {
            throw new IllegalStateException("Table " + fromItem.toString() + " not a real table");
        }
        Table fromTable = (Table) fromItem;
        if (getTable(fromTable.getName()) == null) {
            throw new IllegalStateException("Table " + fromTable.getName() + " not found");
        }
    }
}

