package dubstep.storage;

import net.sf.jsqlparser.schema.Table;
import net.sf.jsqlparser.statement.create.table.ColumnDefinition;
import net.sf.jsqlparser.statement.create.table.CreateTable;

import java.io.*;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;



public class DubTable {

    private String tableName;
    List<ColumnDefinition> columnDefinitions;
    String dataFile;
    public List<datatypes> typeList;
    private int rowCount = -1;

    public DubTable(CreateTable createTable) {
        tableName = createTable.getTable().getName();
        columnDefinitions =  createTable.getColumnDefinitions();
        dataFile = "data/" + tableName + ".csv";
        typeList = new ArrayList<>();
        for(int i =0 ; i < columnDefinitions.size();i++)
        {
            String dataType = columnDefinitions.get(i).getColDataType().getDataType().toLowerCase();
            if(dataType.equals("int"))
                typeList.add(datatypes.INT_TYPE);
            if(dataType.equals("date"))
                typeList.add(datatypes.DATE_TYPE);
            if(dataType.equals("decimal"))
                typeList.add(datatypes.DOUBLE_TYPE);
            if(dataType.equalsIgnoreCase("string") || dataType.equalsIgnoreCase("varchar") || dataType.equalsIgnoreCase("char"))
                typeList.add(datatypes.STRING_TYPE);

        }
        countNumRows();
    }

    private final void countNumRows() {
        File file = new File(dataFile);
        if (!file.exists()) {
            throw new IllegalStateException("Data file doesn't exist for table = " + tableName);
        }
        int lines = 0;
        try {
            BufferedReader reader = new BufferedReader(new FileReader(dataFile));
            while (reader.readLine() != null) lines++;
            reader.close();
        } catch (IOException e) {
            e.printStackTrace();
        }
        rowCount = lines;
    }

    public String GetTableName() {
        return this.tableName;
    }

    public Map<String, Integer> getColumnList(Table fromItem) {
        String alias = fromItem.getAlias();
        String fromName = alias == null ? this.tableName : alias;
        Map<String, Integer> columns = new HashMap<>();
        for (int i = 0; i < columnDefinitions.size(); ++i) {
            columns.put(fromName + "." + columnDefinitions.get(i).getColumnName(), i);
        }
        return columns;
    }

    public int getRowCount() {
        return rowCount;
    }
}
