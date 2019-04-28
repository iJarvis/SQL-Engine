package dubstep.storage;

import dubstep.utils.QueryTimer;
import dubstep.utils.Tuple;
import net.sf.jsqlparser.expression.*;

import java.io.*;
import java.nio.file.FileSystems;
import java.nio.file.Path;
import java.sql.Date;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.List;



public class Scanner {

    DubTable scanTable;
    //    Lock tableLock = new Lock();
    BufferedReader tableReader;
    Integer currentMaxTid;
    ArrayList<DataInputStream> colsDis = new ArrayList<>();
    ArrayList<ObjectInputStream> colsOis = new ArrayList<>();


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
            String path = "data/"+scanTable.GetTableName()+"/cols/";

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

        ArrayList<String> fileBuffer = new ArrayList<>();
        int numCols = scanTable.columnDefinitions.size();
        List<datatypes> typeList= scanTable.typeList;
        parseTimer.start();
        for(int i =0 ; i < tupleCount;i++)
        {
            ArrayList<PrimitiveValue> valueArray = new ArrayList<>();
            for(int j =0 ; j < numCols;j++)
            {
                PrimitiveValue value = null;
                try {
                switch (typeList.get(j))
                {
                    case DATE_TYPE:
                            String dsr = colsDis.get(j).readLine();
                            value = new DateValue(dsr);
                        break;
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
                    parseTimer.stop();
                    return true;
                }
                valueArray.add(value);
            }
            parseTimer.stop();
            tupleBuffer.add(new Tuple(valueArray));
        }
        return false;
    }


}



