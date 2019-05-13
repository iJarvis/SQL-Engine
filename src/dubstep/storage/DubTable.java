package dubstep.storage;

import dubstep.utils.Tuple;
import net.sf.jsqlparser.expression.DateValue;
import net.sf.jsqlparser.expression.PrimitiveValue;
import net.sf.jsqlparser.schema.Table;
import net.sf.jsqlparser.statement.create.table.ColumnDefinition;
import net.sf.jsqlparser.statement.create.table.CreateTable;
import net.sf.jsqlparser.statement.create.table.Index;

import java.io.*;
import java.util.*;

public class DubTable {

    public List<datatypes> typeList;
    public ArrayList<ArrayList<PrimitiveValue>> memCols;
    public ArrayList<ArrayList<Long>> dateCols;
    public List<String> primaryColumns = new ArrayList<>();
    public Long[] isDeleted;
    public HashSet<Integer> deletedSet = new HashSet<Integer>();
    public HashMap<Integer,Tuple> updatedSet = new HashMap<>();
    List<ColumnDefinition> columnDefinitions;
    public List<DataOutputStream> InsertStreams = new ArrayList<>();
    String dataFile;
    private String tableName;
    public int rowCount = -1;

    public DubTable(CreateTable createTable) {
        tableName = createTable.getTable().getName();
        columnDefinitions = createTable.getColumnDefinitions();
        dataFile = "data/" + tableName + ".csv";
        typeList = new ArrayList<>();
        memCols = new ArrayList<>();
        dateCols = new ArrayList<>();
        for (int i = 0; i < columnDefinitions.size(); i++) {
            String dataType = columnDefinitions.get(i).getColDataType().getDataType().toLowerCase();
            memCols.add(new ArrayList<>());
            dateCols.add(new ArrayList<>());
            if (dataType.equals("int"))
                typeList.add(datatypes.INT_TYPE);
            if (dataType.equals("date")) {
                typeList.add(datatypes.DATE_TYPE);
            }
            if (dataType.equals("decimal"))
                typeList.add(datatypes.DOUBLE_TYPE);
            if (dataType.equalsIgnoreCase("string") || dataType.equalsIgnoreCase("varchar") || dataType.equalsIgnoreCase("char"))
                typeList.add(datatypes.STRING_TYPE);

        }
        List<Index> indexes = createTable.getIndexes();
        if (indexes != null && indexes.size() != 0) {
            primaryColumns.add(indexes.get(0).getName());
        }
        postProcessCreate();
    }

    private final void postProcessCreate() {
        File file = new File(dataFile);
        String tableDir = "split/" + tableName;
        String path = tableDir + "/cols/";
        File processed = new File(path + "/exists.txt");
        if (!file.exists()) {
            throw new IllegalStateException("Data file doesn't exist for table = " + tableName);
        }

        if (!processed.exists()) {
            System.out.println(path);
            new File(path).mkdirs();
            System.out.println(processed.getName());

            try {
                System.out.println(processed.getPath());
                processed.createNewFile();
                List<DataOutputStream> cols_files = new ArrayList<DataOutputStream>();
                int index = 0;
                for (datatypes type : typeList) {
                    FileOutputStream file_ptr = new FileOutputStream(path + "/" + index);
                    DataOutputStream stream = new DataOutputStream(new BufferedOutputStream(file_ptr, 8000));
                    index++;
                    cols_files.add(stream);
                }
                splitTuplesAndWrite(cols_files);
                for (DataOutputStream stream : cols_files) {
                    stream.close();
                }

                index = 0;
                for (datatypes type : typeList) {
                    FileOutputStream file_ptr = new FileOutputStream(path + "/" + index,true);
                    DataOutputStream stream = new DataOutputStream(new BufferedOutputStream(file_ptr, 8000));
                    InsertStreams.add(stream);
                    index++;
                }
            } catch (Exception e) {
                e.printStackTrace();
                System.exit(0);
            }
        } else {
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
        isDeleted = new Long[(rowCount/64)+1];
        for (int i = 0; i < (rowCount/64)+1; ++i) {
        isDeleted[i] = 0L;
    }
}

    private void splitTuplesAndWrite(List<DataOutputStream> colFiles) throws Exception {

        DateValue val = new DateValue("2019-10-10");
        java.util.Date d = val.getValue();
        Long time = d.getTime();
        java.sql.Date d1 = new java.sql.Date(time);
        val.setValue(d1);


        BufferedReader reader = new BufferedReader(new FileReader(dataFile));
        ArrayList<ObjectOutputStream> oosList = new ArrayList<>();
        String line = reader.readLine();
        int counter = 0;
        Boolean isFirst = true;
        while (line != null) {
            Tuple tuple = new Tuple(line, counter, typeList);
            int index = 0;
            for (PrimitiveValue t : tuple.valueArray) {
                datatypes type = typeList.get(index);
                switch (type) {
                    case DATE_TYPE:
                        java.sql.Date dd = ((DateValue) t).getValue();
                        Long t1 = dd.getTime();
                        colFiles.get(index).writeLong(t1);
                        break;
                    case INT_TYPE:
                        colFiles.get(index).writeLong(t.toLong());

                        break;
                    case DOUBLE_TYPE:
                        colFiles.get(index).writeDouble(t.toDouble());
                        break;
                    case STRING_TYPE:
                        colFiles.get(index).writeBytes(t.toRawString() + "\n");
                        break;
                }
                tuple.valueArray[index] = null;
                index++;
            }
            counter++;
            line = reader.readLine();
        }
        rowCount = counter;
    }

    public String GetTableName() {
        return this.tableName;
    }

    public Map<String, Integer> getColumnList() {

        String fromName = this.tableName;
        Map<String, Integer> columns = new HashMap<>();
        for (int i = 0; i < columnDefinitions.size(); ++i) {
            columns.put(fromName + "." + columnDefinitions.get(i).getColumnName(), i);
        }
        return columns;
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
    public Map<String, Integer> getColumnList1(Table fromItem) {
        String alias = fromItem.getAlias();
        String fromName = alias == null ? this.tableName : alias;
        Map<String, Integer> columns = new HashMap<>();
        for (int i = 0; i < columnDefinitions.size(); ++i) {
            columns.put(fromName + "." + columnDefinitions.get(i).getColumnName(), i);
            columns.put(columnDefinitions.get(i).getColumnName(), i);
        }
        return columns;
    }

    public int getRowCount() {
        return rowCount;
    }
}
