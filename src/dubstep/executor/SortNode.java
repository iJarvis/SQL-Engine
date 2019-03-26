package dubstep.executor;

import dubstep.Main;
import dubstep.utils.Tuple;
import net.sf.jsqlparser.expression.PrimitiveValue;
import net.sf.jsqlparser.schema.Column;
import net.sf.jsqlparser.schema.PrimitiveType;
import net.sf.jsqlparser.statement.select.OrderByElement;

import java.util.ArrayList;
import java.util.Comparator;
import java.util.List;

public class SortNode extends BaseNode {

    private static final String filename = "%s_%s";

    private List<OrderByElement> elems;
    private boolean sortDone = false;
    private List<Tuple> sortBuffer = new ArrayList<>();
    private int idx = 0;

    public SortNode(List<OrderByElement> elems, BaseNode innerNode) {
        super();
        this.innerNode = innerNode;
        this.elems = elems;
    }

    private void performSort() {
        if (Main.mySchema.isInMem()) {
            Tuple nextTuple = innerNode.getNextTuple();
            while (nextTuple != null) {
                sortBuffer.add(nextTuple);
                nextTuple = innerNode.getNextTuple();
            }
            TupleComparator comparator = new TupleComparator(elems);
            sortBuffer.sort(comparator);
        } else {
            throw new UnsupportedOperationException("External sort not supported yet");
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
            throw new UnsupportedOperationException("External sort not supported yet");
        }
    }

    @Override
    void resetIterator() {

    }

    @Override
    void initProjectionInfo() {
        this.projectionInfo = this.innerNode.projectionInfo;
    }

    private class TupleComparator implements Comparator<Tuple> {

        private List<OrderByElement> elems;

        public TupleComparator(List<OrderByElement> elems) {
            this.elems = elems;
        }

        @Override
        public int compare(Tuple left, Tuple right) {
            try {
                for (int i = 0; i < elems.size(); ++i) {
                    boolean isAsc = elems.get(i).isAsc();
                    if (! (elems.get(i).getExpression() instanceof Column)) {
                        throw new UnsupportedOperationException("Column isn't of type Column");
                    }
                    PrimitiveValue leftPV = left.getValue((Column) elems.get(i).getExpression(), projectionInfo);
                    PrimitiveValue righPV = right.getValue((Column) elems.get(i).getExpression(), projectionInfo);
                    if (leftPV.getType() == PrimitiveType.DOUBLE) {
                        if (leftPV.toDouble() < righPV.toDouble()) {
                            return isAsc? -1: 1;
                        } else if (leftPV.toDouble() > righPV.toDouble()) {
                            return isAsc? 1: -1;
                        }
                    } else if (leftPV.getType() == PrimitiveType.LONG) {
                        if (leftPV.toLong() < righPV.toLong()) {
                            return isAsc? -1: 1;
                        } else if (leftPV.toLong() > righPV.toLong()) {
                            return isAsc? 1: -1;
                        }
                    }
                }
            } catch (PrimitiveValue.InvalidPrimitive e) {
                e.printStackTrace();
            }
            return 0;
        }
    }
}
