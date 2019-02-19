package dubstep.storage;

import dubstep.utils.Tuple;
import net.sf.jsqlparser.statement.create.table.ColumnDefinition;
import net.sf.jsqlparser.statement.create.table.CreateTable;
import sun.misc.Lock;

import java.io.BufferedReader;
import java.io.FileNotFoundException;
import java.io.FileReader;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

public class DubTable {

    String tableName;
    ArrayList<ColumnDefinition> columnDefinitions;
    String dataFile;
    Lock tableLock = new Lock();
    BufferedReader tableReader;
    Integer currentMaxTid;

    public DubTable(CreateTable createTable) {
        tableName = createTable.getTable().getName();
        columnDefinitions = (ArrayList) createTable.getColumnDefinitions();
        dataFile = "data/" + tableName + ".dat";
    }

    void lockTable() {
        try {
            tableLock.lock();
        } catch (InterruptedException e) {
            System.out.println("unable to lock DubTable " + this.tableName);
            e.printStackTrace();
        }
    }

    void unlockTable() {
        tableLock.unlock();
    }

    public boolean initRead() {
        try {
            this.tableReader = new BufferedReader(new FileReader(dataFile));
            this.currentMaxTid = 0;
        } catch (FileNotFoundException e) {
            System.out.println("datafile node found" + this.dataFile);

        }
        return (tableReader == null);

    }

    public void ResetRead()
    {
        try {
            this.tableReader.reset();
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    public boolean readTuples(int tupleCount, ArrayList<Tuple> tupleBuffer) {

        int readTuples = 0;
        int TidStart = this.currentMaxTid;

        ArrayList<String> fileBuffer = new ArrayList<>();

        this.lockTable();

        if (this.tableReader == null) {
            System.err.println("Stop !! - DubTable read not initialized or DubTable read already complete");
            this.unlockTable();
            return true;
        } else {

            try {
                String line;

                while (tupleCount > readTuples && (line = this.tableReader.readLine()) != null) {
                    fileBuffer.add(line);
                    readTuples++;
                }
                this.currentMaxTid += readTuples;
                if (readTuples != tupleCount) {
                    this.tableReader.close();
                    this.currentMaxTid = 0;
                }
                this.tableLock.unlock();
                tupleBuffer.clear();
                convertToTuples(tupleBuffer, fileBuffer, TidStart, tupleCount, readTuples);
                fileBuffer.clear();
            } catch (IOException e) {
                e.printStackTrace();
            }
        }
        return (!(readTuples == tupleCount));
    }

    void convertToTuples(ArrayList<Tuple> tupleBuffer, ArrayList<String> fileBuffer, int tidStart, int tupleCount, int readTuples) {
        for (String tupleString : fileBuffer) {
            tupleBuffer.add(new Tuple(tupleString, tupleCount++, this.columnDefinitions));
        }
    }

    public String GetTableName()
    {
        return this.tableName;
    }

    public ArrayList<String> GetColumnList()
    {
        ArrayList<String> columnList = new ArrayList<>() ;
        for (ColumnDefinition columnDefinition : this.columnDefinitions)
        {
           columnList.add( this.tableName+"."+columnDefinition.getColumnName());

        }
        return  columnList;
    }
}
