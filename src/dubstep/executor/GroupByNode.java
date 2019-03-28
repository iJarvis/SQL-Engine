package dubstep.executor;

import dubstep.utils.Aggregate;
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

public class GroupByNode extends BaseNode{
    private HashMap<ArrayList<PrimitiveValue>, ArrayList<AggregateMap>> buffer;
    private Evaluator evaluator;
    private ArrayList<SelectExpressionItem> selectExpressionItems;
    //private ArrayList<Aggregate> aggObjects;
    private Boolean isInit = false;
    private Tuple next;
    //private Boolean done;

    public GroupByNode(BaseNode innernode, ArrayList<SelectExpressionItem> selectExpressionItems){
        this.innerNode = innernode;
        this.selectExpressionItems = selectExpressionItems;
        this.initProjectionInfo();
        this.evaluator = new Evaluator(this.projectionInfo);
        this.next = null;
        buffer = new HashMap<ArrayList<PrimitiveValue>, ArrayList<AggregateMap>>();
        this.fillBuffer();
    }

    public Tuple getNextRow(){
        if (this.buffer == null)
            return null;
        if(this.buffer.isEmpty()){
            return null;
        }

        ArrayList<PrimitiveValue> result = buffer.keySet().iterator().next();
        ArrayList<AggregateMap> mapList = buffer.get(result);
        buffer.remove(result);

        for(AggregateMap map: mapList){
            result.set(map.index, map.aggregate.getCurrentResult());
        }

        return new Tuple(result);
    }

    private void fillBuffer(){
        if (!this.isInit){
            next = this.innerNode.getNextTuple();
            this.isInit = true;
        }

        ArrayList<Expression> selectExpressions = new ArrayList<>();

        for (SelectExpressionItem expressionItems:selectExpressionItems){
            selectExpressions.add(expressionItems.getExpression());
        }

        ArrayList <PrimitiveValue> rowValues = new ArrayList<PrimitiveValue>(selectExpressions.size());

        while(next != null){
            this.evaluator.setTuple(next);
            ArrayList<Integer> indices = new ArrayList<Integer>();

            for(int i = 0; i < selectExpressions.size(); i++){
                if(selectExpressions.get(i) instanceof Column){
                    try{
                        rowValues.set(i, evaluator.eval(selectExpressions.get(i)));
                    }catch(SQLException e){
                        e.printStackTrace();
                    }
                }else {
                    indices.add(i);
                }
            }

            if(buffer.containsKey(rowValues)){
                for (AggregateMap pair : buffer.get(rowValues)){
                    pair.aggregate.yield(next);
                }
                continue;
            }

            ArrayList<AggregateMap> mapList = new ArrayList<AggregateMap>();
            Aggregate aggregate;

            for (int i : indices){
                aggregate = Aggregate.getAggObject((Function) selectExpressions.get(i), evaluator);
                aggregate.yield(next);
                AggregateMap map = new AggregateMap(i, aggregate);
                mapList.add(map);
            }

            buffer.put(rowValues, mapList);
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
    void resetIterator(){
        this.innerNode.resetIterator();
        this.fillBuffer();
    }

    private class AggregateMap{
        private int index;
        private Aggregate aggregate;

        private AggregateMap(int index, Aggregate aggregate){
            this.index = index;
            this.aggregate = aggregate;
        }
    }
}