package dubstep.executor;

import dubstep.utils.Tuple;
import net.sf.jsqlparser.expression.*;
import net.sf.jsqlparser.schema.Column;
import net.sf.jsqlparser.expression.PrimitiveValue;
import net.sf.jsqlparser.expression.PrimitiveValue.InvalidPrimitive;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.Iterator;
import java.util.LinkedList;

import static dubstep.executor.BaseNode.DataType.*;

public class HashJoinNode extends BaseNode {

    private Expression filter;
    private DataType condType = DataType.NONE;
    private Iterator<Tuple> leftTupleIterator;
    private Column rightCol;
    private HashMap<Object, LinkedList<Tuple>> hashJoinTable;
    private boolean initJoin = false;
    private Tuple outerTuple;

    public HashJoinNode(BaseNode innerNode,BaseNode outerNode, Expression filter) {

        this.innerNode = innerNode;
        this.innerNode.parentNode = this;
        this.outerNode = outerNode;
        this.outerNode.parentNode = this;
        this.filter = filter;
        this.initProjectionInfo();
        this.initHashMap();
    }

    public void initHashMap(){

        this.hashJoinTable = new HashMap<Object, LinkedList<Tuple>>();
        this.condType = NONE; //Check BaseNode for definition

        BinaryExpression binaryExpression = (BinaryExpression) this.filter;
        Column leftColumn = (Column) binaryExpression.getLeftExpression();
        Column rightColumn = (Column) binaryExpression.getRightExpression();
        Column tempColumn;
        Tuple innerTuple = innerNode.getNextTuple();
        PrimitiveValue innerTupleValue = innerTuple.getValue(leftColumn, this.innerNode.projectionInfo);

        if (innerTupleValue == null){       // if column is not found in one of the two children, swap left and right columns.
            tempColumn = rightColumn;
            rightColumn = leftColumn;
            leftColumn = tempColumn;
        }

        this.rightCol = rightColumn;
        int i = 1;
        while(innerTuple != null){

            innerTupleValue = innerTuple.getValue(leftColumn, innerNode.projectionInfo);
            if (condType == NONE){
                if (innerTupleValue instanceof LongValue)
                    condType = LONG;
                else if (innerTupleValue instanceof DoubleValue)
                    condType = DOUBLE;
                else if (innerTupleValue instanceof DateValue)
                    condType = DATE;
                else if (innerTupleValue instanceof StringValue)
                    condType = STRING;
            }

            Object castedValue = getCastedValue(innerTupleValue);

            LinkedList<Tuple> values;

            if (hashJoinTable.containsKey(castedValue)){
                values = hashJoinTable.get(castedValue);
            } else {
                values = new LinkedList<Tuple>();
                hashJoinTable.put(castedValue, values);
            }
            values.add(innerTuple);

            innerTuple = innerNode.getNextTuple();
        }

    }

    private Object getCastedValue(PrimitiveValue value){
        try{
            if (condType == LONG)
                return value.toLong();
            else if (condType == DOUBLE)
                return value.toDouble();
            else if (condType == DATE)
                return ((DateValue)value).getValue().getTime();
            else
                return value.toString();
        }catch (InvalidPrimitive error){
            error.printStackTrace();
        }
        return null;
    }

    Tuple getNextRow(){

        if (hashJoinTable == null)
            return null;

        if (!this.initJoin) {
            outerTuple = outerNode.getNextTuple();
            this.initJoin = true;
        }

        if (outerTuple == null && leftTupleIterator == null) {
            this.resetIterator();
            return null;
        }

        PrimitiveValue outerTupleValue;
        while (outerTuple != null){
            if (leftTupleIterator == null){

                outerTupleValue = outerTuple.getValue(this.rightCol, outerNode.projectionInfo);
                Object castedValue = getCastedValue(outerTupleValue);

                if(hashJoinTable.containsKey(castedValue)) {
                    leftTupleIterator = hashJoinTable.get(castedValue).iterator();
                }else{
                    outerTuple = outerNode.getNextTuple();
                }

            }else {
                if(leftTupleIterator.hasNext())
                    return new Tuple(leftTupleIterator.next(), outerTuple);
                leftTupleIterator = null;
                outerTuple = outerNode.getNextTuple();
            }
        }

        this.resetIterator();
        return null;
    }

    @Override
    void resetIterator() {
        this.hashJoinTable = null;
        this.condType = NONE;
        this.innerNode.resetIterator();
        this.outerNode.resetIterator();
    }

    void initProjectionInfo() {
        this.projectionInfo = new ArrayList<>(this.innerNode.projectionInfo);
        this.projectionInfo.addAll(this.outerNode.projectionInfo);
    }
}
