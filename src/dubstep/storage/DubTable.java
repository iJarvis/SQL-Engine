package dubstep.storage;

import dubstep.utils.Tuple;
import net.sf.jsqlparser.expression.PrimitiveValue;
import net.sf.jsqlparser.schema.Table;
import net.sf.jsqlparser.statement.create.table.ColumnDefinition;
import net.sf.jsqlparser.statement.create.table.CreateTable;

import java.awt.*;
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
    public ArrayList<ArrayList<PrimitiveValue>> memCols;

    public DubTable(CreateTable createTable) {
        tableName = createTable.getTable().getName();
        columnDefinitions =  createTable.getColumnDefinitions();
        dataFile = "data/" + tableName + ".csv";
        typeList = new ArrayList<>();
        memCols = new ArrayList<>();
        for(int i =0 ; i < columnDefinitions.size();i++)
        {
            String dataType = columnDefinitions.get(i).getColDataType().getDataType().toLowerCase();
            memCols.add(new ArrayList<>());
            if(dataType.equals("int"))
                typeList.add(datatypes.INT_TYPE);
            if(dataType.equals("date"))
                typeList.add(datatypes.DATE_TYPE);
            if(dataType.equals("decimal"))
                typeList.add(datatypes.DOUBLE_TYPE);
            if(dataType.equalsIgnoreCase("string") || dataType.equalsIgnoreCase("varchar") || dataType.equalsIgnoreCase("char"))
                typeList.add(datatypes.STRING_TYPE);

        }
        postProcessCreate();
    }

    private final void postProcessCreate() {
        File file = new File(dataFile);
        String path =   tableName + "/cols/";
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
            } catch (Exception e) {
                e.printStackTrace();
                System.exit(0);
            }
        } 
        else {
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
    }

    private void splitTuplesAndWrite(List<DataOutputStream> colFiles) throws Exception {
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
                        if(!tableName.equals("LINEITEM"))
                        memCols.get(index).add(t);
                        colFiles.get(index).writeBytes(t.toString() + "\n");
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
                tuple.valueArray.set(index,null);
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
