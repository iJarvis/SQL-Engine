package dubstep.storage;

import dubstep.utils.QueryTimer;
import dubstep.utils.Tuple;
import net.sf.jsqlparser.expression.*;
import net.sf.jsqlparser.schema.Column;

import java.io.*;
import java.nio.file.FileSystems;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;


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

        datatypes type;
        ArrayList<Tuple> tupleList;
        DataInputStream dis;
        Integer count;
        Integer tupleIndex;
        Boolean finished = false;


        public void run() {
            int current = 0;
            count = tupleList.size();
            while (count > current) {
                PrimitiveValue value = null;
                try {

                    switch (type) {
                        case DATE_TYPE:
                        case INT_TYPE:
                            value = new LongValue(dis.readLong());
                            break;
                        case DOUBLE_TYPE:
                            value = new DoubleValue(dis.readDouble());
                            break;
                        case STRING_TYPE:
                            value = new StringValue(dis.readLine());
                    }
                } catch (Exception e) {
                    finished = true;
                    return;
                }
                tupleList.get(current).valueArray.set(tupleIndex, value);
                current++;

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
        Boolean readComplete = false;
        tupleBuffer.clear();

        ArrayList<String> fileBuffer = new ArrayList<>(scanTable.columnDefinitions.size());
        int numCols = scanTable.columnDefinitions.size();
        List<datatypes> typeList= scanTable.typeList;
        for(int i =0 ; i < tupleCount;i++)
        {
            ArrayList<PrimitiveValue> valueArray = new ArrayList<>();
            if(currentMaxTid >= scanTable.getRowCount()) {
                readComplete = true;
                break;
            }
            for(int j =0 ; j < numCols;j++)
            {

                valueArray.add(null);

            }
            currentMaxTid++;
            tupleBuffer.add(new Tuple(valueArray));
        }
        Boolean isComplete = false;

        int current_index = 0;
        int numThread = 3;
        while (!isComplete)
        {
            ArrayList<TupleConverter> threads= new ArrayList<>();
            for(int i =0 ; i <numThread;i++)
            {
                while (current_index < projVector.size() && projVector.get(current_index)== false )
                {
                    current_index++;
                }
                if(current_index < projVector.size())
                {
                    TupleConverter thread = new TupleConverter();
                    thread.tupleList = tupleBuffer;
                    thread.type = typeList.get(current_index);
                    thread.finished = false;
                    thread.tupleIndex = current_index;
                    thread.dis = colsDis.get(current_index);
                    threads.add(thread);
                    current_index++;
                }
                else {
                    isComplete = true;
                }

            }
            for(int i =0 ; i< threads.size();i++)
            {
                threads.get(i).run();
            }
            for(int i =0 ; i < threads.size();i++)
            {
                try {
                    threads.get(i).join();
                } catch (InterruptedException e) {
                    e.printStackTrace();
                }
            }
            if(isComplete)
                break;

        }

        return readComplete;
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



