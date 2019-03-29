package dubstep.executor;

import dubstep.utils.Aggregate;
import dubstep.utils.Evaluator;
import dubstep.utils.Tuple;
import net.sf.jsqlparser.expression.Function;
import net.sf.jsqlparser.expression.PrimitiveValue;
import net.sf.jsqlparser.expression.Expression;
import net.sf.jsqlparser.statement.select.SelectExpressionItem;
import net.sf.jsqlparser.statement.select.SelectItem;

import java.util.ArrayList;
import java.util.List;

public class AggNode extends BaseNode {
    private Evaluator evaluator;
    private List<SelectItem> selectItems;
    private ArrayList<Aggregate> aggObjects;
    private Boolean isInit = false;
    private Tuple next;
    private Boolean done;

    public AggNode(BaseNode innerNode, List<SelectItem> selectItems){
        this.innerNode = innerNode;
        this.innerNode.parentNode = this;
        this.selectItems = selectItems;
        this.initProjectionInfo();
        this.evaluator = new Evaluator(this.projectionInfo);

        //this.selectExpressions = selectExpressions;
        this.done = false;
        this.aggObjects = new ArrayList<Aggregate>();
        this.initAggNode();
    }

    private void initAggNode(){
        ArrayList<Expression> selectExpressions = new ArrayList<>();

        for (SelectItem expressionItem:selectItems){
            if(expressionItem instanceof SelectExpressionItem) {
                SelectExpressionItem expr = (SelectExpressionItem) expressionItem;
                selectExpressions.add(expr.getExpression());
            }
            else
            {
                throw new UnsupportedOperationException("Plain aggregate only expression is expected");
            }
        }

        for (Expression exp : selectExpressions){
            Function func = (Function) exp;
            aggObjects.add(Aggregate.getAggObject(func, evaluator));
        }
    }

    @Override
    public Tuple getNextRow() {
        if(this.done == true){
            this.resetIterator();
            return null;
        }


        ArrayList <PrimitiveValue> rowValues = new ArrayList<PrimitiveValue>(this.selectItems.size());

        if (!isInit) {
            isInit = true;
            next = innerNode.getNextTuple();
        }

        int i = 0;
        while (next != null){
            next = innerNode.getNextTuple();
            for (i = 0; i < this.selectItems.size(); i++){
                rowValues.set(i, aggObjects.get(i).yield(next)) ;
            }
        }

        this.done = true;
        this.aggObjects = null;
        return new Tuple(rowValues);
    }

    @Override
    void resetIterator() {
        this.done = false;
        this.initAggNode();
        this.innerNode.resetIterator();
    }

    @Override
    void initProjectionInfo() {
        projectionInfo = new ArrayList<>();
        for (SelectItem selectItem : selectItems) {
            String columnName = ((SelectExpressionItem) selectItem).getExpression().toString();
            String alias = ((SelectExpressionItem) selectItem).getAlias();
            if (alias == null) {
                projectionInfo.add(columnName);
            } else {
                projectionInfo.add(alias);
            }
        }
    }
}