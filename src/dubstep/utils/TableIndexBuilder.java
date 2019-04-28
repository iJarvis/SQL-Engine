package dubstep.utils;

import dubstep.Main;
import dubstep.executor.ScanNode;
import dubstep.storage.datatypes;
import net.sf.jsqlparser.expression.*;
import net.sf.jsqlparser.schema.Table;
import net.sf.jsqlparser.statement.create.table.Index;
import net.sf.jsqlparser.statement.select.FromItem;

import java.io.*;
import java.util.*;

public class TableIndexBuilder {

    private static File splitDir = new File("split");

    public static void build(FromItem fromItem) {
        Table table = (Table) fromItem;
        String tableName = table.getName();
        File outputDir = new File(splitDir, tableName);
        if (!outputDir.exists()) {
            outputDir.mkdirs();
        } else {
            //index already exists
            return;
        }

        final int maxBufferSize = 50000;
        Map<String, Map<PrimitiveValue, List<Pair<Integer, Long>>>> indexMap = new HashMap<>();

        List<Index> indexes = Main.mySchema.getTable(tableName).getIndexes();
        ScanNode scanNode = new ScanNode(fromItem, null, Main.mySchema);

        int currentIndex = 0;
        File outFile = new File(outputDir, String.valueOf(currentIndex));
        BufferedWriter writer = null;
        try {
            writer = new BufferedWriter(new FileWriter(outFile));
        } catch (IOException e) {
            e.printStackTrace();
        }
        Tuple nextTuple = scanNode.getNextTuple();
        long offset = 0;
        int numTuples = 0;
        while (nextTuple != null) {
            for (Index index: indexes) {
                String indexName = index.getColumnsNames().get(0);
                PrimitiveValue primitiveValue = nextTuple.getValue(tableName + "." + indexName, "", scanNode.projectionInfo);
                if (!indexMap.containsKey(indexName)) {
                    indexMap.put(indexName, new HashMap<>());
                }
                Map<PrimitiveValue, List<Pair<Integer, Long>>> offsetMap = indexMap.get(indexName);
                if (!offsetMap.containsKey(primitiveValue)) {
                    offsetMap.put(primitiveValue, new ArrayList<>());
                }
                offsetMap.get(primitiveValue).add(new Pair<>(currentIndex, offset));
            }
            try {
                String s = nextTuple.serializeTuple() + "\n";
                writer.write(s);
                offset += s.length();
            } catch (IOException e) {
                e.printStackTrace();
            }
            ++numTuples;
            nextTuple = scanNode.getNextTuple();
            if (numTuples == maxBufferSize) {
                ++currentIndex;
                outFile = new File(outputDir, String.valueOf(currentIndex));
                try {
                    writer.close();
                    if (nextTuple != null) {
                        writer = new BufferedWriter(new FileWriter(outFile));
                    }
                } catch (IOException e) {
                    e.printStackTrace();
                }
                offset = 0;
                numTuples = 0;
            }
        }
        try {
            writer.close();
        } catch (IOException e) {
            e.printStackTrace();
        }

        Main.mySchema.getTable(tableName).setIndexMap(indexMap);

        for (Index index: indexes) {
            Properties properties = new Properties();
            String indexName = index.getColumnsNames().get(0);
            File outputIndexFile = new File(outputDir, indexName + ".index");
            for (Map.Entry<PrimitiveValue, List<Pair<Integer, Long>>> entry : indexMap.get(indexName).entrySet()) {
                StringBuilder sb = new StringBuilder();
                for (Pair<Integer, Long> pair : entry.getValue()) {
                    sb.append(pair.getElement0());
                    sb.append(".");
                    sb.append(pair.getElement1());
                    sb.append(".");
                }
                properties.put(entry.getKey().toRawString(), sb.toString());
            }
//            Map<String, List<Pair<Integer, Long>>> offsetMap = indexMap.get(indexName);
//            try {
//                FileOutputStream fileOut = new FileOutputStream(outputIndexFile);
//                ObjectOutputStream out = new ObjectOutputStream(fileOut);
//                out.writeObject(offsetMap);
//                out.close();
//                fileOut.close();
//            } catch (IOException e) {
//                e.printStackTrace();
//            }
            try {
                properties.store(new FileOutputStream(outputIndexFile), null);
            } catch (IOException e) {
                e.printStackTrace();
            }
        }
    }
}
