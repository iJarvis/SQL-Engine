package dubstep.executor;

import dubstep.Main;
import dubstep.utils.Pair;
import dubstep.utils.Tuple;
import dubstep.utils.TupleComparator;
import dubstep.utils.Utils;
import net.sf.jsqlparser.expression.PrimitiveValue;
import net.sf.jsqlparser.schema.Column;
import net.sf.jsqlparser.statement.create.table.ColumnDefinition;
import net.sf.jsqlparser.statement.select.OrderByElement;

import java.io.*;
import java.util.*;

public class SortNode extends BaseNode {

    private static final String tempDir = "temp";
    private static final int NUMBER_OF_TUPLES_IN_MEM = 5;

    private List<OrderByElement> elems;
    private boolean sortDone = false;
    private List<Tuple> sortBuffer = new ArrayList<>();
    private int idx = 0;
    private TupleOrderByComparator comparator;
    private PriorityQueue<Pair<Integer, Tuple>> queue = null;
    private List<BufferedReader> brList = null;
    private List<ColumnDefinition> columnDefinitions = null;

    public SortNode(List<OrderByElement> elems, BaseNode innerNode) {
        super();
        this.innerNode = innerNode;
        this.elems = elems;
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
        File currentSortDir = new File(temp, String.valueOf(Utils.getRandomNumber(0, 100)));
        if (!currentSortDir.exists()) {
            currentSortDir.mkdirs();
        }
        Tuple nextTuple = innerNode.getNextTuple();
        if (nextTuple != null) {
            columnDefinitions = nextTuple.getColumnDefinitions();
        }
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
                    writer.write(sortBuffer.get(i) + "\n");
                }

                writer.close();

                ++fileCount;
            }

            brList = new ArrayList<>();
            queue = new PriorityQueue<>(getPQComparator());

            for (int i = 0; i < fileCount; ++i) {
                File sortFile = new File(currentSortDir, String.valueOf(i));
                BufferedReader br = new BufferedReader(new FileReader(sortFile));
                Tuple tuple = new Tuple(br.readLine(), -1, columnDefinitions);
                Pair<Integer, Tuple> pair = new Pair<>(i, tuple);
                queue.add(pair);
                brList.add(br);
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
                String str = brList.get(pair.getElement0()).readLine();
                if (str != null) {
                    Tuple tuple = new Tuple(str, -1, columnDefinitions);
                    Pair<Integer, Tuple> newPair = new Pair<>(pair.getElement0(), tuple);
                    queue.add(newPair);
                } else {
                    brList.get(pair.getElement0()).close();
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
        this.projectionInfo = this.innerNode.projectionInfo;
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
