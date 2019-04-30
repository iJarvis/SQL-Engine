package dubstep.executor;

import dubstep.utils.Tuple;
import dubstep.utils.Utils;
import net.sf.jsqlparser.expression.*;
import net.sf.jsqlparser.schema.Column;
import net.sf.jsqlparser.expression.PrimitiveValue;
import net.sf.jsqlparser.expression.PrimitiveValue.InvalidPrimitive;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.Iterator;
import java.util.LinkedList;

import static dubstep.executor.BaseNode.DataType.*;
import static dubstep.planner.PlanTree.getSelectExprColumnList;
import static dubstep.planner.PlanTree.getSelectExprColumnStrList;

public class HashJoinNode extends BaseNode {

    public Expression filter;
    private DataType condType = DataType.NONE;
    private Iterator<Tuple> leftTupleIterator;
    private Column rightCol;
    private Column rightCol1;
    private HashMap<Object, LinkedList<Tuple>> hashJoinTable;
    private boolean initJoin = false;
    private Tuple outerTuple;
    private  boolean isInit = false;
    private boolean isEmpty = false;
    private Expression filter1 =  null;

    public HashJoinNode(BaseNode innerNode,BaseNode outerNode, Expression filter) {

        this.innerNode = innerNode;
        this.innerNode.parentNode = this;
        this.outerNode = outerNode;
        this.outerNode.parentNode = this;
        this.filter = filter;
        this.initProjectionInfo();

    }

    public void initHashMap(){
        if(this.parentNode instanceof SelectNode)
        {
            SelectNode sel = (SelectNode) this.parentNode;
            filter1 = sel.filter;

        }

        this.hashJoinTable = new HashMap<>();
        this.condType = NONE; //Check BaseNode for definition

        BinaryExpression binaryExpression = (BinaryExpression) filter;

        Column leftColumn = (Column) binaryExpression.getLeftExpression();
        Column rightColumn = (Column) binaryExpression.getRightExpression();
        Column tempColumn;
        Tuple innerTuple = innerNode.getNextTuple();
        if(innerTuple == null) {
            isEmpty = true;
            return;
        }
        PrimitiveValue innerTupleValue = innerTuple.getValue(leftColumn, innerNode.projectionInfo);

        if (innerTupleValue == null){       // if column is not found in one of the two children, swap left and right columns.
            tempColumn = rightColumn;
            rightColumn = leftColumn;
            leftColumn = tempColumn;
        }
        Column leftColumn1 = null,rightColumn1=null ;
        PrimitiveValue innerTupleValue1 = null;
        if(filter1 != null) {
            BinaryExpression binaryExpression1 = (BinaryExpression) filter1;

            leftColumn1 = (Column) binaryExpression1.getLeftExpression();
            rightColumn1 = (Column) binaryExpression1.getRightExpression();
            Column tempColumn1;

            innerTupleValue1 = innerTuple.getValue(leftColumn, innerNode.projectionInfo);


            if (innerTupleValue == null) {       // if column is not found in one of the two children, swap left and right columns.
                tempColumn1 = rightColumn1;
                rightColumn1 = leftColumn1;
                leftColumn1 = tempColumn1;
            }
            this.rightCol1 = rightColumn1;
        }
        this.rightCol = rightColumn;

        int i = 1;

        while(innerTuple != null){

            innerTupleValue = innerTuple.getValue(leftColumn, innerNode.projectionInfo);
            if(leftColumn1 != null)
                innerTupleValue1 = innerTuple.getValue(leftColumn1,innerNode.projectionInfo);
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

            String castedValue = innerTupleValue.toString();
            if(this.filter1 != null)
                castedValue += innerTupleValue1.toString();

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
                return value.toString();
            else if (condType == DOUBLE)
                return value.toString();
            else if (condType == DATE)
                return ((DateValue)value).getValue().getTime();
            else
                return value.toString();
        }catch (Exception error){
            error.printStackTrace();
        }
        return null;
    }

    Tuple getNextRow(){
        if(isInit == false)
        {
            isInit = true;
            this.initHashMap();
            if(isEmpty)
                return null;
        }

        if (hashJoinTable == null)
            return null;

        if (!this.initJoin) {
            outerTuple = outerNode.getNextTuple();
            this.initJoin = true;
        }

        if (outerTuple == null && leftTupleIterator == null) {
            this.hashJoinTable = null;
            this.condType = NONE;
            return null;
        }

        PrimitiveValue outerTupleValue;
        PrimitiveValue outerTupleValue1;
        while (outerTuple != null){
            if (leftTupleIterator == null){

                outerTupleValue = outerTuple.getValue(this.rightCol, outerNode.projectionInfo);


                String castedValue = outerTupleValue.toString();
                if(this.rightCol1!=null)
                {
                    outerTupleValue1 = outerTuple.getValue(this.rightCol1,outerNode.projectionInfo);
                    castedValue += outerTupleValue1;
                }


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
        this.hashJoinTable = null;

        this.hashJoinTable = null;
        this.condType = NONE;
        this.resetIterator();
        return null;
    }

    @Override
    void resetIterator() {
        this.hashJoinTable = null;
        this.isInit = false;
        innerNode.resetIterator();
        outerNode.resetIterator();
    }

    void initProjectionInfo() {
        projectionInfo = new HashMap<>(innerNode.projectionInfo);
        Utils.mapPutAll(outerNode.projectionInfo, projectionInfo);
    }

    @Override
    public void initProjPushDownInfo() {
        this.requiredList.addAll(this.parentNode.requiredList);
        this.requiredList.addAll((ArrayList)getSelectExprColumnStrList(filter));
        this.innerNode.initProjPushDownInfo();
        this.outerNode.initProjPushDownInfo();
    }
}
