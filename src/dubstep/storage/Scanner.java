package dubstep.storage;

import dubstep.utils.Tuple;

import java.io.BufferedReader;
import java.io.FileNotFoundException;
import java.io.FileReader;
import java.io.IOException;
import java.nio.file.FileSystems;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.List;

public class Scanner {

    DubTable scanTable;
    //    Lock tableLock = new Lock();
    BufferedReader tableReader;
    Integer currentMaxTid;

    public Scanner(DubTable table)
    {
        scanTable = table;

    }


    public boolean initRead() {
        try {
            this.tableReader = new BufferedReader(new FileReader(scanTable.dataFile));
            this.currentMaxTid = 0;
        } catch (FileNotFoundException e) {
            System.out.println("datafile not found : " + scanTable.dataFile);
            Path path = FileSystems.getDefault().getPath(".");
            System.out.println("Current working directory : " + path.toAbsolutePath());
        }
        return (tableReader == null);

    }


    public void resetRead() {
        initRead();
    }

    public boolean readTuples(int tupleCount, ArrayList<Tuple> tupleBuffer) {

        int readTuples = 0;
        int TidStart = this.currentMaxTid;

        ArrayList<String> fileBuffer = new ArrayList<>();

        if (this.tableReader == null) {
            System.err.println("Stop !! - DubTable read not initialized or DubTable read already complete");

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
//                this.tableLock.unlock();
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
            tupleBuffer.add(new Tuple(tupleString, tupleCount++, scanTable.columnDefinitions));
        }
    }

}
