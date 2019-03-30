package dubstep.executor;

import dubstep.Aggregate.Aggregate;
import dubstep.utils.Evaluator;
import dubstep.utils.Tuple;
import net.sf.jsqlparser.expression.Expression;
import net.sf.jsqlparser.expression.Function;
import net.sf.jsqlparser.expression.PrimitiveValue;
import net.sf.jsqlparser.schema.Column;
import net.sf.jsqlparser.statement.select.SelectExpressionItem;
import net.sf.jsqlparser.statement.select.SelectItem;

import java.sql.SQLException;
import java.util.ArrayList;
import java.util.HashMap;

public class GroupByNode extends BaseNode {
    private HashMap<Object, ArrayList<AggregateMap>> buffer;
    private Evaluator evaluator;
    private ArrayList<SelectExpressionItem> selectExpressionItems;
    //private ArrayList<Aggregate> aggObjects;
    private Boolean isInit = false;
    private Tuple next;
    //private Boolean done;

    public GroupByNode(BaseNode innernode, ArrayList<SelectExpressionItem> selectExpressionItems) {
        this.innerNode = innernode;
        this.selectExpressionItems = selectExpressionItems;
        this.initProjectionInfo();
        this.evaluator = new Evaluator(this.innerNode.projectionInfo);
        this.next = null;
        this.fillBuffer();
    }

    public Tuple getNextRow() {

        if (this.buffer == null) {
            return null;
        }
        if (this.buffer.isEmpty()) {
            this.buffer = null;
            return null;
        }
        Tuple result = (Tuple) buffer.keySet().iterator().next();
        ArrayList<AggregateMap> mapList = buffer.get(result);
        buffer.remove(result);

        for (AggregateMap map : mapList) {
            result.setValue(map.index, map.aggregate.getCurrentResult());
        }

        return (result);
    }

    private void fillBuffer() {

        this.buffer = new HashMap<Object, ArrayList<AggregateMap>>();

        if (!this.isInit) {
            next = this.innerNode.getNextTuple();
            this.isInit = true;
        }

        ArrayList<Expression> selectExpressions = new ArrayList<>();

        for (SelectExpressionItem expressionItems : selectExpressionItems) {
            selectExpressions.add(expressionItems.getExpression());
        }

        PrimitiveValue[] rowValues = new PrimitiveValue[selectExpressionItems.size()];
        Tuple keyRow = new Tuple(rowValues);

        while (next != null) {
            this.evaluator.setTuple(next);
            ArrayList<Integer> indices = new ArrayList<Integer>();

            for (int i = 0; i < selectExpressions.size(); i++) {
                if (selectExpressions.get(i) instanceof Column) {
                    try {
                        keyRow.setValue(i, evaluator.eval(selectExpressions.get(i)));
                    } catch (SQLException e) {
                        e.printStackTrace();
                    }
                } else {
                    indices.add(i);
                }
            }

            if (buffer.containsKey(keyRow)) {
                for (AggregateMap pair : buffer.get(keyRow)) {
                    pair.aggregate.yield(next);
                }
                next = this.innerNode.getNextTuple();
                continue;
            }

            ArrayList<AggregateMap> mapList = new ArrayList<AggregateMap>();
            Aggregate aggregate;

            for (int i : indices) {
                aggregate = Aggregate.getAggObject((Function) selectExpressions.get(i), evaluator);
                aggregate.yield(next);
                AggregateMap map = new AggregateMap(i, aggregate);
                mapList.add(map);
            }
            buffer.put(keyRow, mapList);
            next = this.innerNode.getNextTuple();
        }
    }

    @Override
    void initProjectionInfo() {
        projectionInfo = new ArrayList<>();
        for (SelectItem selectItem : selectExpressionItems) {
            String columnName = ((SelectExpressionItem) selectItem).getExpression().toString();
            String alias = ((SelectExpressionItem) selectItem).getAlias();
            if (alias == null) {
                projectionInfo.add(columnName);
            } else {
                projectionInfo.add(alias);
            }
        }
    }

    @Override
    void resetIterator() {
        this.innerNode.resetIterator();
        this.fillBuffer();
    }

    private class AggregateMap {
        private int index;
        private Aggregate aggregate;

        private AggregateMap(int index, Aggregate aggregate) {
            this.index = index;
            this.aggregate = aggregate;
        }
    }
}