package dubstep.executor;

import dubstep.Aggregate.Aggregate;
import dubstep.utils.Evaluator;
import dubstep.utils.Tuple;
import net.sf.jsqlparser.expression.Expression;
import net.sf.jsqlparser.expression.Function;
import net.sf.jsqlparser.expression.PrimitiveValue;
import net.sf.jsqlparser.statement.select.SelectExpressionItem;
import net.sf.jsqlparser.statement.select.SelectItem;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;

public class AggNode extends BaseNode {
    private Evaluator evaluator;
    private ArrayList<SelectExpressionItem> selectExpressionItems;
    //private ArrayList<Expression> selectExpressions;
    private ArrayList<Aggregate> aggObjects;
    private Boolean isInit = false;
    private Tuple next;
    private Boolean done;

    public AggNode(BaseNode innerNode, ArrayList<SelectExpressionItem> selectExpressionItems) {
        this.innerNode = innerNode;
        this.innerNode.parentNode = this;
        this.selectExpressionItems = selectExpressionItems;
        this.done = false;
        this.initProjectionInfo();
        this.evaluator = new Evaluator(this.innerNode.projectionInfo);
        this.initAggNode();
    }

    private void initAggNode() {
        this.aggObjects = new ArrayList<Aggregate>();
        ArrayList<Expression> selectExpressions = new ArrayList<>();

        for (SelectExpressionItem expressionItems : selectExpressionItems) {
            selectExpressions.add(expressionItems.getExpression());
        }

        for (Expression exp : selectExpressions) {
            Function func = (Function) exp;
            aggObjects.add(Aggregate.getAggObject(func, this.evaluator));
        }
    }

    @Override
    public Tuple getNextRow() {
        if (done) {
            resetIterator();
            return null;
        }

        List<PrimitiveValue> rowValues = new ArrayList<>();

        if (!isInit) {
            isInit = true;
            next = innerNode.getNextTuple();
        }

        int i = 0;
        while (next != null) {
            for (i = 0; i < selectExpressionItems.size(); i++) {
                rowValues.add(aggObjects.get(i).yield(next));
            }
            next = innerNode.getNextTuple();
        }

        done = true;
        aggObjects = null;
        if (rowValues.size() != 0) {
            return new Tuple(rowValues);
        }
        return null;
    }

    @Override
    void resetIterator() {
        done = false;
        aggObjects = null;
        innerNode.resetIterator();
        initAggNode();
    }

    @Override
    void initProjectionInfo() {
        this.projectionInfo = new HashMap<>();
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
}