package dubstep.storage;

import dubstep.utils.Tuple;
import net.sf.jsqlparser.schema.Table;
import net.sf.jsqlparser.statement.create.table.ColumnDefinition;
import net.sf.jsqlparser.statement.create.table.CreateTable;

import java.io.BufferedReader;
import java.io.FileNotFoundException;
import java.io.FileReader;
import java.io.IOException;
import java.nio.file.FileSystems;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.List;

public class DubTable {

    String tableName;
    List<ColumnDefinition> columnDefinitions;
    String dataFile;

    public DubTable(CreateTable createTable) {
        tableName = createTable.getTable().getName();
        columnDefinitions =  createTable.getColumnDefinitions();
        dataFile = "data/" + tableName + ".csv";
    }



    public String GetTableName() {
        return this.tableName;
    }

    public ArrayList<String> GetColumnList(Table fromItem) {
        String alias = fromItem.getAlias();
        String fromName = alias == null ? this.tableName : alias;
        ArrayList<String> columnList = new ArrayList<>();
        for (ColumnDefinition columnDefinition : this.columnDefinitions) {
            columnList.add(fromName + "." + columnDefinition.getColumnName());
        }
        return columnList;
    }
}
