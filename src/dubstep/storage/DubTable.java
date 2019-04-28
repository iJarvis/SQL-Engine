package dubstep.storage;

import dubstep.utils.Pair;
import net.sf.jsqlparser.expression.PrimitiveValue;
import net.sf.jsqlparser.schema.Table;
import net.sf.jsqlparser.statement.create.table.ColumnDefinition;
import net.sf.jsqlparser.statement.create.table.CreateTable;
import net.sf.jsqlparser.statement.create.table.Index;

import java.io.*;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;


public class DubTable {

    private String tableName;
    List<ColumnDefinition> columnDefinitions;
    private Index primaryIndex;
    private String dataFile;
    public List<datatypes> typeList;
    private int rowCount = -1;
    private List<Index> indexes;
    private Map<String, Map<PrimitiveValue, List<Pair<Integer, Long>>>> indexMap;

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
//        countNumRows();
    }

    public void setIndexes(List<Index> indexes) {
        this.indexes = indexes;
    }

    public List<Index> getIndexes() {
        return indexes;
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

    public String getDataFile() {
        return dataFile;
    }

    public void setDataFile(String dataFile) {
        this.dataFile = dataFile;
        System.out.println(dataFile);
    }

    public void setPrimaryIndex(Index primaryIndex) {
        this.primaryIndex = primaryIndex;
    }

    public Index getPrimaryIndex() {
        return primaryIndex;
    }

    public void setIndexMap(Map<String, Map<PrimitiveValue, List<Pair<Integer, Long>>>> indexMap) {
        this.indexMap = indexMap;
    }

    public Map<String, Map<PrimitiveValue, List<Pair<Integer, Long>>>> getIndexMap() {
        return indexMap;
    }
}
