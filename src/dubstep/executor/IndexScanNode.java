package dubstep.executor;

import dubstep.Main;
import dubstep.storage.DubTable;
import dubstep.utils.BPlusTree;
import dubstep.utils.Pair;
import dubstep.utils.Tuple;
import net.sf.jsqlparser.expression.BinaryExpression;
import net.sf.jsqlparser.expression.Expression;
import net.sf.jsqlparser.expression.PrimitiveValue;
import net.sf.jsqlparser.expression.operators.relational.GreaterThan;
import net.sf.jsqlparser.schema.Column;
import net.sf.jsqlparser.schema.PrimitiveType;
import net.sf.jsqlparser.schema.Table;
import net.sf.jsqlparser.statement.select.FromItem;

import java.io.File;
import java.text.SimpleDateFormat;
import java.util.*;

public class IndexScanNode extends BaseNode {

    private static SimpleDateFormat dateFormat = new SimpleDateFormat("yyyy-MM-dd");

    ArrayList<Pair<Double, List<Pair<Integer, Long>>>> listDouble = new ArrayList<>();
    ArrayList<Pair<String, List<Pair<Integer, Long>>>> listString = new ArrayList<>();
    ArrayList<Pair<Long, List<Pair<Integer, Long>>>> listLong = new ArrayList<>();

    public IndexScanNode(FromItem fromItem, Column column, PrimitiveValue lowerBound, PrimitiveValue upperBound) {
        super();
        if (!(fromItem instanceof Table)) {
            throw new UnsupportedOperationException("Scan node call without table");
        }
        DubTable table = Main.mySchema.getTable(((Table) fromItem).getName());
        Map<PrimitiveValue, List<Pair<Integer, Long>>> indexMap = table.getIndexMap().get(column.getColumnName());
        PrimitiveType type;
        if (lowerBound != null) {
            type = lowerBound.getType();
        } else {
            type = upperBound.getType();
        }
        try {
            for (Map.Entry<PrimitiveValue, List<Pair<Integer, Long>>> entry: indexMap.entrySet()) {
                if (type == PrimitiveType.LONG) {
                    listLong.add(new Pair<>(entry.getKey().toLong(), entry.getValue()));
                    listLong.sort(new Comparator<Pair<Long, List<Pair<Integer, Long>>>>() {
                        @Override
                        public int compare(Pair<Long, List<Pair<Integer, Long>>> left, Pair<Long, List<Pair<Integer, Long>>> right) {
                            if (left.getElement0().equals(right.getElement0())) {
                                return 0;
                            } else {
                                return left.getElement0() < right.getElement0() ? -1 : 1;
                            }
                        }
                    });
                } else if (type == PrimitiveType.DOUBLE) {
                    listDouble.add(new Pair<>(entry.getKey().toDouble(), entry.getValue()));
                    listDouble.sort(new Comparator<Pair<Double, List<Pair<Integer, Long>>>>() {
                        @Override
                        public int compare(Pair<Double, List<Pair<Integer, Long>>> left, Pair<Double, List<Pair<Integer, Long>>> right) {
                            if (left.getElement0().equals(right.getElement0())) {
                                return 0;
                            } else {
                                return left.getElement0() < right.getElement0() ? -1 : 1;
                            }
                        }
                    });
                } else if (type == PrimitiveType.DATE) {
                    listString.add(new Pair<>(entry.getKey().toRawString(), entry.getValue()));
                    listString.sort(new Comparator<Pair<String, List<Pair<Integer, Long>>>>() {
                        @Override
                        public int compare(Pair<String, List<Pair<Integer, Long>>> left, Pair<String, List<Pair<Integer, Long>>> right) {
                            if (left.getElement0().equals(right.getElement0())) {
                                return 0;
                            } else {
                                return left.getElement0().compareTo(right.getElement0());
                            }
                        }
                    });
                } else {
                    throw new UnsupportedOperationException("Can't handle this type in IndexScan");
                }
            }
        } catch (PrimitiveValue.InvalidPrimitive e) {
            e.printStackTrace();
        }

    }

    @Override
    Tuple getNextRow() {
        return null;
    }

    @Override
    void resetIterator() {

    }

    @Override
    void initProjectionInfo() {
    }

    @Override
    public void initProjPushDownInfo() {

    }
}
