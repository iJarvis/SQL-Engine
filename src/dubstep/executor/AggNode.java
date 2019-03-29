package dubstep.executor;

import dubstep.Aggregate.Aggregate;
import dubstep.utils.Evaluator;
import dubstep.utils.Tuple;
import net.sf.jsqlparser.expression.Function;
import net.sf.jsqlparser.expression.PrimitiveValue;
import net.sf.jsqlparser.expression.Expression;
import net.sf.jsqlparser.statement.select.SelectExpressionItem;
import net.sf.jsqlparser.statement.select.SelectItem;

import java.util.ArrayList;

public class AggNode extends BaseNode {
    private Evaluator evaluator;
    private ArrayList<SelectExpressionItem> selectExpressionItems;
    //private ArrayList<Expression> selectExpressions;
    private ArrayList<Aggregate> aggObjects;
    private Boolean isInit = false;
    private Tuple next;
    private Boolean done;

    public AggNode(BaseNode innerNode, ArrayList<SelectExpressionItem> selectExpressionItems){
        this.innerNode = innerNode;
        this.innerNode.parentNode = this;
        this.selectExpressionItems = selectExpressionItems;
        //this.selectExpressions = selectExpressions;
        this.done = false;
        this.initProjectionInfo();
        this.evaluator = new Evaluator(this.innerNode.projectionInfo);
        this.initAggNode();
    }

    private void initAggNode(){
        this.aggObjects = new ArrayList<Aggregate>();
        ArrayList<Expression> selectExpressions = new ArrayList<>();

        for (SelectExpressionItem expressionItems:selectExpressionItems){
            selectExpressions.add(expressionItems.getExpression());
        }

        //this.selectExpressionItems = selectExpressionItems;

        for (Expression exp : selectExpressions){
            Function func = (Function) exp;
            aggObjects.add(Aggregate.getAggObject(func, this.evaluator));
        }
    }

    @Override
    public Tuple getNextRow() {
        if(this.done){
            this.resetIterator();
            return null;
        }

        PrimitiveValue[] rowValues = new PrimitiveValue[selectExpressionItems.size()];

        if (!this.isInit) {
            this.isInit = true;
            this.next = innerNode.getNextTuple();
        }

        int i = 0;
        while (this.next != null) {
            for (i = 0; i < selectExpressionItems.size(); i++){
                rowValues[i] =  aggObjects.get(i).yield(this.next);
            }
            this.next = innerNode.getNextTuple();
        }

        this.done = true;
        this.aggObjects = null;
        return new Tuple(rowValues);
    }

    @Override
    void resetIterator() {
        this.done = false;
        this.aggObjects = null;
        this.innerNode.resetIterator();
        this.initAggNode();
    }

    @Override
    void initProjectionInfo() {
        this.projectionInfo = new ArrayList<>();
        for (SelectItem selectItem : selectExpressionItems) {
            String columnName = ((SelectExpressionItem) selectItem).getExpression().toString();
            String alias = ((SelectExpressionItem) selectItem).getAlias();
            if (alias == null) {
                this.projectionInfo.add(columnName);
            } else {
                this.projectionInfo.add(alias);
            }
        }
    }
}