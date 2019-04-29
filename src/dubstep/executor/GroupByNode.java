package dubstep.executor;

import dubstep.Aggregate.Aggregate;
import dubstep.Main;
import dubstep.utils.Evaluator;
import dubstep.utils.Tuple;
import net.sf.jsqlparser.expression.*;
import net.sf.jsqlparser.schema.Column;
import net.sf.jsqlparser.statement.select.OrderByElement;
import net.sf.jsqlparser.statement.select.SelectExpressionItem;
import net.sf.jsqlparser.statement.select.SelectItem;

import java.sql.SQLException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;

import static dubstep.planner.PlanTree.getSelectExprColumnList;
import static dubstep.planner.PlanTree.getSelectExprColumnStrList;
import static dubstep.utils.Tuple.deserializeTuple;

public class GroupByNode extends BaseNode {
    private HashMap<String, ArrayList<AggregateMap>> buffer;
    private Evaluator evaluator;
    private ArrayList<SelectExpressionItem> selectExpressionItems;
    private ArrayList<Expression> selectExpressions = new ArrayList<>();
    private Aggregate[] aggObjects;
    private Boolean isInit = false;
    private Boolean isInitColDef = false;
    private Boolean resetAgg = false;
    private Tuple next;
    private List<Column> groupbyCols = new ArrayList<>();
    private ArrayList<Integer> aggIndices;
   // private Column groupByCol;
    private String refCol;
    private  boolean isFilled = false;
    //private Boolean done;

    public GroupByNode(BaseNode innernode, ArrayList<SelectExpressionItem> selectExpressionItems) {
        this.innerNode = innernode;
        this.aggIndices = new ArrayList<>();
        this.innerNode.parentNode = this;
        this.selectExpressionItems = selectExpressionItems;
        for (SelectExpressionItem expressionItems : selectExpressionItems) {
            this.selectExpressions.add(expressionItems.getExpression());
        }
        for (int i = 0; i < selectExpressions.size(); i++) {
            if (!(selectExpressions.get(i) instanceof Column)) {
                this.aggIndices.add(i);
            }
        }
        this.aggObjects = null;
        this.initProjectionInfo();
        this.evaluator = new Evaluator(this.innerNode.projectionInfo);
        this.next = null;
        this.refCol = "";
         if (Main.mySchema.isInMem()){
        }
        else {
           this.generateSortNode();
        }
    }

    public Tuple getNextRow() {
        if (Main.mySchema.isInMem()) {
            if(!isFilled)
            {
                isFilled = true;
                this.fillBuffer();
            }
            return inMemNextRow();
        }
        else
           return onDiskNextRow();

    }

    public Tuple onDiskNextRow() {

        if (this.aggObjects == null) {
            this.initAggObjects();
        }
        if (!this.isInit) {
            next = this.innerNode.getNextTuple();
            this.isInit = true;

            if (next != null) {
                this.refCol = "";
                for (Column groupCol : groupbyCols) {
                    this.refCol = this.refCol + next.getValue(groupCol, this.innerNode.projectionInfo).toString();
                }
            }
        }
        if (resetAgg) {
            resetAgg = false;
            for (int i : aggIndices) {
                aggObjects[i].resetCurrentResult();
            }
        }

        String curCol = "";
        ArrayList<PrimitiveValue> rowValues = new ArrayList<>(selectExpressions.size());
        for (int i = 0; i < selectExpressions.size(); i++) {
            rowValues.add(null);
        }

        while (next != null) {

            this.evaluator.setTuple(next);
            curCol = "";
            for (Column groupCol : groupbyCols) {
                curCol = curCol + next.getValue(groupCol, this.innerNode.projectionInfo).toString();
            }

            if (!(curCol.equals(refCol))) {
                resetAgg = true;
                refCol = curCol;
                return new Tuple(rowValues);
            }

            for (int i = 0; i < selectExpressions.size(); i++) {
                if (selectExpressions.get(i) instanceof Column) {
                    try {
                        rowValues.set(i,evaluator.eval(selectExpressions.get(i)));
                    } catch (SQLException e) {
                        e.printStackTrace();
                    }
                } else {
                    rowValues.set(i,aggObjects[i].yield(next));
                }
            }
            next = this.innerNode.getNextTuple();
        }

        if(rowValues.get(0) != null)
            return new Tuple(rowValues);
        return null;
    }

    public void initAggObjects() {

        aggObjects = new Aggregate[selectExpressions.size()];
        for (int i : aggIndices) {
            aggObjects[i] = Aggregate.getAggObject((Function) selectExpressions.get(i), evaluator);
        }
    }

    public Tuple inMemNextRow() {

        if (this.buffer == null) {
            return null;
        }
        if (this.buffer.isEmpty()) {
            this.buffer = null;
            return null;
        }
        String resString = buffer.keySet().iterator().next();
        Tuple result = deserializeTuple(resString);

        ArrayList<AggregateMap> mapList = buffer.get(resString);
        buffer.remove(resString);

        for (AggregateMap map : mapList) {
            result.addValueItem(map.aggregate.getCurrentResult());
        }
        int valueStartIndex = 0;
        int aggIndicesIndex = result.valueArray.size() - aggIndices.size();
        int currentAgg = 0;

        ArrayList<PrimitiveValue> valueArray = new ArrayList<>();
        for(int i =0 ; i<result.valueArray.size() ;i++)
        {
            if(currentAgg < aggIndices.size() && aggIndices.get(currentAgg) == i )
            {
                currentAgg++;
                valueArray.add(result.valueArray.get(aggIndicesIndex++));
            }
            else
                valueArray.add(result.valueArray.get(valueStartIndex++));
        }
        result.valueArray = valueArray;
        return (result);
    }

    private void fillBuffer() {

        this.buffer = new HashMap<>();

        if (!this.isInit) {
            next = this.innerNode.getNextTuple();
            this.isInit = true;
        }

        if (next == null)
            return;

        while (next != null) {
            this.evaluator.setTuple(next);
            ArrayList<PrimitiveValue> rowValues = new ArrayList<>();

            for (int i = 0; i < selectExpressions.size(); i++) {
                Expression selectExpression = selectExpressions.get(i);
                if (selectExpression instanceof Column) {
                    try {
                         rowValues.add(evaluator.eval(selectExpression));
                    } catch (SQLException e) {
                        e.printStackTrace();
                    }
                }
            }
            Tuple keyRow = new Tuple(rowValues);
            String keyString = keyRow.serializeTuple();

            if (buffer.containsKey(keyString)) {
                for (AggregateMap pair : buffer.get(keyString)) {
                    pair.aggregate.yield(next);
                }
                next = this.innerNode.getNextTuple();
                continue;
            }

            ArrayList<AggregateMap> mapList = new ArrayList<AggregateMap>();
            Aggregate aggregate;

            for (int i : aggIndices) {
                aggregate = Aggregate.getAggObject((Function) selectExpressions.get(i), evaluator);
                aggregate.yield(next);
                AggregateMap map = new AggregateMap(i, aggregate);
                mapList.add(map);
            }
            buffer.put(keyString, mapList);
            next = this.innerNode.getNextTuple();
        }
    }

    public void generateSortNode() {

        List<OrderByElement> elems = new ArrayList<>();
        OrderByElement sortElement = null;

        for (Expression selectExpression : selectExpressions) {
            if (selectExpression instanceof Column) {
                this.groupbyCols.add((Column) selectExpression);
                sortElement = new OrderByElement();
                sortElement.setExpression((Expression) selectExpression);
                elems.add(sortElement);
                sortElement.setAsc(true);
            }
        }
        this.innerNode = new SortNode(elems, this.innerNode);
        this.innerNode.parentNode = this;
    }

    @Override
    void initProjectionInfo() {
        projectionInfo = new HashMap<>();
        for (int i = 0; i < selectExpressionItems.size(); ++i) {
            SelectItem selectItem = selectExpressionItems.get(i);
            String columnName = ((SelectExpressionItem) selectItem).getExpression().toString();
            String alias = ((SelectExpressionItem) selectItem).getAlias();
            if (alias == null) {
                projectionInfo.put(columnName, i);
            } else {
                projectionInfo.put(alias, i);
            }
        }
    }

    @Override
    public void initProjPushDownInfo() {
        this.requiredList.addAll(this.parentNode.requiredList);
        for(int i =0 ; i < selectExpressionItems.size();i++)
        {
            this.requiredList.addAll((ArrayList)getSelectExprColumnStrList(selectExpressionItems.get(i).getExpression()));
        }
        this.innerNode.initProjPushDownInfo();

    }

    @Override
    void resetIterator() {
        this.innerNode.resetIterator();
        this.aggObjects = null;
        this.isInit = false;
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