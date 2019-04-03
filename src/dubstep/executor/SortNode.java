package dubstep.executor;

import dubstep.Main;
import dubstep.utils.Pair;
import dubstep.utils.Tuple;
import dubstep.utils.TupleComparator;
import dubstep.utils.Utils;
import net.sf.jsqlparser.expression.PrimitiveValue;
import net.sf.jsqlparser.schema.Column;
import net.sf.jsqlparser.statement.select.OrderByElement;

import java.io.*;
import java.util.*;

public class SortNode extends BaseNode {

    private static final String tempDir = "temp";
    private static final int NUMBER_OF_TUPLES_IN_MEM = 100;

    private boolean sortDone = false;
    private List<Tuple> sortBuffer = new ArrayList<>();
    private int idx = 0;
    private TupleOrderByComparator comparator;
    private PriorityQueue<Pair<Integer, Tuple>> queue = null;
    private File currentSortDir;

    public SortNode(List<OrderByElement> elems, BaseNode innerNode) {
        super();
        this.innerNode = innerNode;
        this.innerNode.parentNode = this;
        comparator = new TupleOrderByComparator(elems);
        initProjectionInfo();
    }

    private void performSort() {
        if (Main.mySchema.isInMem()) {
            sortBuffer.clear();
            Tuple nextTuple = innerNode.getNextTuple();
            while (nextTuple != null) {
                sortBuffer.add(nextTuple);
                nextTuple = innerNode.getNextTuple();
            }
            sortBuffer.sort(comparator);
        } else {
            performExternalSort();
        }
    }

    private void performExternalSort() {
        File temp = new File(tempDir);
        currentSortDir = new File(temp, String.valueOf(Utils.getRandomNumber(0, 10000)));
        if (!currentSortDir.exists()) {
            currentSortDir.mkdirs();
        } else {
            Utils.deleteDir(currentSortDir);
            currentSortDir.mkdirs();
        }
        Tuple nextTuple = innerNode.getNextTuple();
        try {
            int fileCount = 0;
            while (nextTuple != null) {
                sortBuffer.clear();
                for (int i = 0; (i < NUMBER_OF_TUPLES_IN_MEM) && (nextTuple != null); ++i) {
                    sortBuffer.add(nextTuple);
                    nextTuple = innerNode.getNextTuple();
                }
                sortBuffer.sort(comparator);

                File sortFile = new File(currentSortDir, String.valueOf(fileCount));
                BufferedWriter writer = new BufferedWriter(new FileWriter(sortFile));

                for (int i = 0; i < sortBuffer.size(); ++i) {
                    writer.write(sortBuffer.get(i).serializeTuple() + "\n");
                }

                writer.close();

                ++fileCount;
            }

            queue = new PriorityQueue<>(getPQComparator());

            for (int i = 0; i < fileCount; ++i) {
                File sortFile = new File(currentSortDir, String.valueOf(i));
                BufferedReader br = new BufferedReader(new FileReader(sortFile));
                Tuple tuple = Tuple.deserializeTuple(br.readLine());
                br.close();
                Pair<Integer, Tuple> pair = new Pair<>(i, tuple);
                queue.add(pair);
            }
        } catch (IOException e) {
            e.printStackTrace();
        }

    }

    @Override
    Tuple getNextRow() {
        if (!sortDone) {
            performSort();
            sortDone = true;
        }
        if (Main.mySchema.isInMem()) {
            if (idx >= sortBuffer.size()) {
                return null;
            }
            return sortBuffer.get(idx++);
        } else {
            if (queue.isEmpty()) {
                return null;
            }
            try {
                Pair<Integer, Tuple> pair = queue.poll();
                File sortFile = new File(currentSortDir, String.valueOf(pair.getElement0()));
                BufferedReader br = new BufferedReader(new FileReader(sortFile));
                String str = br.readLine();
                br.close();
                if (str != null) {
                    Tuple tuple = Tuple.deserializeTuple(str);
                    Pair<Integer, Tuple> newPair = new Pair<>(pair.getElement0(), tuple);
                    queue.add(newPair);
                }
                return pair.getElement1();
            } catch (Exception e) {
                e.printStackTrace();
            }
        }
        return null;
    }

    @Override
    void resetIterator() {
        if (Main.mySchema.isInMem()) {
            idx = 0;
        }
    }

    @Override
    void initProjectionInfo() {
        projectionInfo = innerNode.projectionInfo;
    }

    private Comparator<Pair<Integer, Tuple>> getPQComparator() {
        return new Comparator<Pair<Integer, Tuple>>() {
            @Override
            public int compare(Pair<Integer, Tuple> left, Pair<Integer, Tuple> right) {
                return comparator.compare(left.getElement1(), right.getElement1());
            }
        };
    }

    private class TupleOrderByComparator implements Comparator<Tuple> {

        private List<OrderByElement> elems;

        public TupleOrderByComparator(List<OrderByElement> elems) {
            this.elems = elems;
        }

        @Override
        public int compare(Tuple left, Tuple right) {
            for (int i = 0; i < elems.size(); ++i) {
                boolean isAsc = elems.get(i).isAsc();
                if (! (elems.get(i).getExpression() instanceof Column)) {
                    throw new UnsupportedOperationException("Column isn't of type Column");
                }
                PrimitiveValue leftPV = left.getValue((Column) elems.get(i).getExpression(), projectionInfo);
                PrimitiveValue righPV = right.getValue((Column) elems.get(i).getExpression(), projectionInfo);
                return TupleComparator.compare(leftPV, righPV, isAsc);
            }
            return 0;
        }
    }
}
