package dubstep.storage;

import dubstep.utils.QueryTimer;
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


    class TupleConverter extends Thread {

        public List<String> input;
        public List<Tuple> output;
        int start_index;
        int end_index;

        public void run()
        {
            for (int i =start_index ; i < end_index; i++) {
                output.add(new Tuple(input.get(i), 0, scanTable.typeList));
            }

        }

    }

    public Scanner(DubTable table)
    {
        scanTable = table;

    }


    public boolean initRead() {
        try {
            this.tableReader = new BufferedReader(new FileReader(scanTable.dataFile));
            this.currentMaxTid = 0;
        } catch (FileNotFoundException e) {
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
                int size = fileBuffer.size();
                parseTimer.start();

                int numThreads = 3;
                ArrayList<TupleConverter> threads = new ArrayList<>();


                for(int i =0 ; i < numThreads;i++)
                {
                    TupleConverter thread = new TupleConverter();
                    thread.input = fileBuffer;
                    thread.start_index = size/numThreads * i;
                    thread.end_index = thread.start_index + size/numThreads;
//                    System.out.println(""+thread.start_index+"  "+thread.end_index);
                    thread.output = new ArrayList<>();
                    if(i == numThreads-1)
                        thread.end_index = size;
                    threads.add(thread);
                    thread.start();

                }
                for(int i =0 ; i < numThreads;i++)
                {
                    try {
                        threads.get(i).join();
                    } catch (InterruptedException e) {
                        e.printStackTrace();
                    }
                    tupleBuffer.addAll(threads.get(i).output);
                }

                parseTimer.stop();
                fileBuffer.clear();
            } catch (IOException e) {
                e.printStackTrace();
            }
        }
        return (!(readTuples == tupleCount));
    }

//    void convertToTuples(ArrayList<Tuple> tupleBuffer, ArrayList<String> fileBuffer,int tupleCount, QueryTimer parseTimer) {
//        for (String tupleString : fileBuffer) {
//            tupleBuffer.add(new Tuple(tupleString, tupleCount++, scanTable.typeList,parseTimer));
//        }
//    }


}



