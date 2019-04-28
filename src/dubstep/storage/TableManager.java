package dubstep.storage;

import dubstep.utils.BPlusTree;
import net.sf.jsqlparser.schema.Table;
import net.sf.jsqlparser.statement.create.table.CreateTable;
import net.sf.jsqlparser.statement.select.FromItem;

import java.util.HashMap;


public class TableManager {

    private HashMap<String, DubTable> tableDirectory = new HashMap<>();
    private HashMap<String, BPlusTree> bpTrees = new HashMap<>();
    private boolean inMem = true;

    public boolean createTable(CreateTable createTable) {
        String tableName = createTable.getTable().getName();
        if (tableDirectory.containsKey(tableName))
            return false;
        DubTable newTable = new DubTable(createTable);
        newTable.setIndexes(createTable.getIndexes());
        tableDirectory.put(tableName, newTable);
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

    public BPlusTree getBPTree(String wholeColumnName) {
        return bpTrees.get(wholeColumnName);
    }

    public void setBPTree(String wholeColumnName, BPlusTree bPlusTree) {
        bpTrees.put(wholeColumnName, bPlusTree);
    }

    public BPlusTree buildBPTreeFromFile(String wholeColumnName) {
        throw new UnsupportedOperationException("Can't build trees yet");
    }
}

