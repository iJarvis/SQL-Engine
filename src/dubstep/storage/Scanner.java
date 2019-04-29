package dubstep.storage;

import dubstep.utils.QueryTimer;
import dubstep.utils.Tuple;
import net.sf.jsqlparser.expression.*;
import net.sf.jsqlparser.schema.Column;

import java.io.*;
import java.nio.file.FileSystems;
import java.nio.file.Path;
import java.sql.Date;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;

import static dubstep.storage.datatypes.DATE_TYPE;


public class Scanner {

    DubTable scanTable;
    //    Lock tableLock = new Lock();
    BufferedReader tableReader;
    Integer currentMaxTid;
    ArrayList<DataInputStream> colsDis = new ArrayList<>();
    ArrayList<ObjectInputStream> colsOis = new ArrayList<>();
    ArrayList<Boolean> projVector = new ArrayList<>();
    ArrayList<ArrayList<DateValue>> datePlaceHolders = new ArrayList<>();


    class TupleConverter extends Thread {

        public List<String> input;
        public List<Tuple> output;
        int start_index;
        int end_index;

        public void run()
        {
            if(scanTable.GetTableName().equals("LINEITEM"))
            {
                for (int i =start_index ; i < end_index; i++) {
                    output.add(new Tuple(input.get(i), scanTable.typeList,true));
                }

            }
            else {
                for (int i = start_index; i < end_index; i++) {
                    output.add(new Tuple(input.get(i), 0, scanTable.typeList));
                }
            }

        }

    }

    public Scanner(DubTable table)
    {
        scanTable = table;

    }


    public boolean initRead() {
        try {
            this.tableReader = new BufferedReader(new FileReader(scanTable.dataFile),25600);
            this.currentMaxTid = 0;
            String path = scanTable.GetTableName()+"/cols/";

            for(int i =0 ; i< scanTable.columnDefinitions.size();i++)
            {
                colsDis.add(new DataInputStream(new BufferedInputStream(new FileInputStream(path+i) )));
            }

        } catch (Exception e) {
            e.printStackTrace();
            Path path = FileSystems.getDefault().getPath(".");
            System.out.println("Current working directory : " + path.toAbsolutePath());
            throw new IllegalStateException("datafile not found : " + scanTable.dataFile);
        }
        return (tableReader == null);

    }


    public void resetRead() {
        initRead();
    }

    public boolean readTuples(int tupleCount, ArrayList<Tuple> tupleBuffer, QueryTimer parseTimer) {
        int readTuples = 0;
        int TidStart = this.currentMaxTid;
        tupleBuffer.clear();

        ArrayList<String> fileBuffer = new ArrayList<>(scanTable.columnDefinitions.size());
        int numCols = scanTable.columnDefinitions.size();
        List<datatypes> typeList= scanTable.typeList;
        Boolean isLineItem = scanTable.GetTableName().equals("LINEITEM");
        for(int i =0 ; i < tupleCount;i++)
        {
            ArrayList<PrimitiveValue> valueArray = new ArrayList<>();
            for(int j =0 ; j < numCols;j++)
            {
                if(currentMaxTid > scanTable.getRowCount()) {
                    return true;
                }
                PrimitiveValue value = null;
                if(projVector.get(j)) {
                    if (scanTable.memCols.get(j).size() > 0 ) {
                        if(currentMaxTid < scanTable.memCols.get(j).size())
                            value = scanTable.memCols.get(j).get(currentMaxTid);
                        else {
                            return true;
                        }
                    }
                    else {
                        try {
                            switch (typeList.get(j)) {
                                case DATE_TYPE:
                                case INT_TYPE:
                                    value =  new LongValue(colsDis.get(j).readLong());
                                    break;
                                case DOUBLE_TYPE:
                                    value =  new DoubleValue(colsDis.get(j).readDouble());
                                    break;
                                case STRING_TYPE:
                                    value = new StringValue(colsDis.get(j).readLine());
                            }
                        } catch (Exception e) {
                            return true;
                        }
                    }
                }
                valueArray.add(value);
            }
            currentMaxTid++;
            tupleBuffer.add(new Tuple(valueArray));
        }
        return false;
    }

    public void setupProjList(HashSet<String> requiredList)
    {

            for(int i =0 ; i < scanTable.columnDefinitions.size();i++)
            {
                String columnName = scanTable.columnDefinitions.get(i).getColumnName();
                String fullColumnName = scanTable.GetTableName()+"."+columnName;
                Column col = new Column();
                if( requiredList.contains(columnName) || requiredList.contains(fullColumnName)) {
                    projVector.add(true);
                        datePlaceHolders.add(null);
                }
                else {
                    projVector.add(false);
                    datePlaceHolders.add(null);
                }
            }
    }

}



