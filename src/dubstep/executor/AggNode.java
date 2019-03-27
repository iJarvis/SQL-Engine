package dubstep.executor;

import dubstep.utils.Aggregate;
import dubstep.utils.Evaluator;
import dubstep.utils.Tuple;
import net.sf.jsqlparser.expression.Function;
import net.sf.jsqlparser.expression.PrimitiveValue;
import net.sf.jsqlparser.expression.Expression;

import java.util.ArrayList;

public class AggNode extends BaseNode {
    private Evaluator evaluator;
    private ArrayList<Expression> selectExpressions;
    private ArrayList<Aggregate> aggObjects;
    private Boolean isInit = false;
    private Tuple next;
    private Boolean done;

    public AggNode(BaseNode innerNode, ArrayList<Expression> selectExpressions){
        this.innerNode = innerNode;
        this.innerNode.parentNode = this;
        this.evaluator = new Evaluator(this.projectionInfo);
        this.selectExpressions = selectExpressions;
        this.initProjectionInfo();
        this.done = false;
        this.aggObjects = new ArrayList<Aggregate>();
    }

    @Override
    public Tuple getNextRow() {
        if(this.done == true){
            this.resetIterator();
            return null;
        }
        ArrayList <PrimitiveValue> rowValues = new ArrayList<PrimitiveValue>();

        for (Expression exp : selectExpressions){
            Function func = (Function) exp;
            aggObjects.add(Aggregate.getAggObject(func, evaluator));
        }

        if (!isInit) {
            isInit = true;
            next = innerNode.getNextTuple();
        }

        int i = 0;
        while (next != null); {
            next = innerNode.getNextTuple();
            for (i = 0; i < selectExpressions.size(); i++){
                rowValues.set(i, aggObjects.get(i).yield(next)) ;
            }
        }

        this.done = true;
        return new Tuple(rowValues);
    }

    @Override
    void resetIterator() {
        this.aggObjects = null;
        this.innerNode.resetIterator();
    }

    @Override
    void initProjectionInfo() {
        this.projectionInfo = this.innerNode.projectionInfo;
    }
}
