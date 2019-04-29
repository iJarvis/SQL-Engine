package dubstep.executor;

import dubstep.Main;
import dubstep.storage.datatypes;
import dubstep.utils.Pair;
import dubstep.utils.Tuple;
import dubstep.utils.TupleComparator;
import dubstep.utils.Utils;
import net.sf.jsqlparser.expression.*;
import net.sf.jsqlparser.schema.Column;
import net.sf.jsqlparser.statement.select.OrderByElement;

import java.io.*;
import java.util.*;

import static dubstep.planner.PlanTree.getSelectExprColumnList;
import static dubstep.planner.PlanTree.getSelectExprColumnStrList;

public class SortNode extends BaseNode {

    private static final String tempDir = "temp";
    private static final int NUMBER_OF_TUPLES_IN_MEM = 10000;

    private boolean sortDone = false;
    private List<Tuple> sortBuffer = new ArrayList<>();
    private int idx = 0;
    private TupleOrderByComparator comparator;
    private List<OrderByElement> orderByElements;
    private PriorityQueue<Pair<Integer, Tuple>> queue = null;
    private File currentSortDir;
    private List<BufferedReader> brList = null;
    private ArrayList<datatypes> serTypes = new ArrayList<>();
    private boolean isTypeInit = false;

    public SortNode(List<OrderByElement> elems, BaseNode innerNode) {
        super();
        this.innerNode = innerNode;
        this.innerNode.parentNode = this;
        comparator = new TupleOrderByComparator(elems);
        initProjectionInfo();
        brList = new ArrayList<>();
        orderByElements = elems;

        //check for edge case that a column in order by clause isn't of the form tablename.columnname where it is so
        //in the select statement
        for (OrderByElement elem: elems) {
            Column column = (Column) elem.getExpression();
            if (!projectionInfo.containsKey(column.getWholeColumnName()) && !projectionInfo.containsKey(column.getColumnName())) {
                for (String key: projectionInfo.keySet()) {
                    String[] parts = key.split("\\.");
                    if (parts.length == 2 && parts[1].equals(column.getWholeColumnName())) {
                        column.setColumnName(key);
                    }
                }
            }
        }
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
                if (!isTypeInit) {
                    isTypeInit = true;
                    Tuple tuple = sortBuffer.get(0);
                    for (PrimitiveValue val : tuple.valueArray) {
                        if (val instanceof LongValue)
                            serTypes.add(datatypes.INT_TYPE);

                        else if (val instanceof StringValue)
                            serTypes.add(datatypes.STRING_TYPE);

                        else if (val instanceof DateValue)
                            serTypes.add(datatypes.DATE_TYPE);

                        else if (val instanceof DoubleValue)
                            serTypes.add(datatypes.DOUBLE_TYPE);
                    }
                }

                for (int i = 0; i < sortBuffer.size(); ++i) {
                    writer.write(sortBuffer.get(i).serializeTuple1() + "\n");
                }

                writer.close();

                ++fileCount;
            }

            queue = new PriorityQueue<>(getPQComparator());

            for (int i = 0; i < fileCount; ++i) {
                File sortFile = new File(currentSortDir, String.valueOf(i));
                BufferedReader br = new BufferedReader(new FileReader(sortFile), 1000);
                Tuple tuple = Tuple.deserializeTuple2(br.readLine(), serTypes);
                brList.add(br);
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
                BufferedReader br = brList.get(pair.getElement0());
                String str = br.readLine();
                if (str != null) {
                    Tuple tuple = Tuple.deserializeTuple2(str, serTypes);
                    Pair<Integer, Tuple> newPair = new Pair<>(pair.getElement0(), tuple);
                    queue.add(newPair);
                } else {
                    br.close();
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
        //TODO: handle this in case of on-disk
    }

    @Override
    void initProjectionInfo() {
        projectionInfo = innerNode.projectionInfo;
    }

    @Override
    public void initProjPushDownInfo() {
        if(this.parentNode !=null)
        this.requiredList.addAll(this.parentNode.requiredList);
        for(int i =0 ; i < orderByElements.size();i++ )
        {
            this.requiredList.addAll((ArrayList)getSelectExprColumnStrList(orderByElements.get(i).getExpression()));
        }

        this.innerNode.initProjPushDownInfo();
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
                if (!(elems.get(i).getExpression() instanceof Column)) {
                    throw new UnsupportedOperationException("Column isn't of type Column");
                }
                PrimitiveValue leftPV = left.getValue((Column) elems.get(i).getExpression(), projectionInfo);
                PrimitiveValue righPV = right.getValue((Column) elems.get(i).getExpression(), projectionInfo);
                int comp = TupleComparator.compare(leftPV, righPV, isAsc);
                if (comp != 0) {
                    return comp;
                }
            }
            return 0;
        }
    }
}
