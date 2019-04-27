package dubstep.storage;

import net.sf.jsqlparser.schema.Table;
import net.sf.jsqlparser.statement.create.table.CreateTable;
import net.sf.jsqlparser.statement.select.FromItem;

import java.util.HashMap;


public class TableManager {

    private HashMap<String, DubTable> tableDirectory = new HashMap<>();
    private boolean inMem = true;

    public boolean createTable(CreateTable createTable) {

        String tableName = createTable.getTable().getName();
        if (tableDirectory.containsKey(tableName))
            return false;
        DubTable newTable = new DubTable(createTable);
        tableDirectory.put(tableName, newTable);
        if (createTable.getIndexes().size() != 0) {
            IndexBuilder indexBuilder = new IndexBuilder(createTable.getTable(), createTable.getIndexes());
            indexBuilder.build();
        }
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

    public void setInMem(boolean inMem){
        this.inMem = inMem;
    }

    public boolean isInMem() {
        return inMem;
    }
}

