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

import java.util.ArrayList;
import java.util.HashMap;

import static dubstep.planner.PlanTree.getSelectExprColumnStrList;

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
            aggObjects = null;
            return null;
        }

        PrimitiveValue[] rowValues = new PrimitiveValue[selectExpressionItems.size()];
        for (int i = 0; i < selectExpressionItems.size(); i++)
            rowValues[i] = (null);
        if (!isInit) {
            isInit = true;
            next = innerNode.getNextTuple();
        }

        while (next != null) {
            for (int i = 0; i < selectExpressionItems.size(); i++) {
                rowValues[i] = (aggObjects.get(i).yield(next));
            }
            next = innerNode.getNextTuple();
        }

        done = true;
        aggObjects = null;
        if (rowValues[0] != null) {
            return new Tuple(rowValues);
        }
        aggObjects = null;
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
        this.typeList = new ArrayList<>(selectExpressionItems.size());
        for (int i = 0; i < selectExpressionItems.size(); ++i) {
            SelectItem selectItem = selectExpressionItems.get(i);
            String columnName = ((SelectExpressionItem) selectItem).getExpression().toString();
            if (((SelectExpressionItem) selectItem).getExpression() instanceof Column)
                typeList.add(i, this.innerNode.typeList.get(innerNode.projectionInfo.get(columnName)));
            else
                typeList.add(i, null);
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
        if (this.parentNode != null)
            this.requiredList.addAll(this.parentNode.requiredList);
        for (int i = 0; i < selectExpressionItems.size(); ++i) {
            this.requiredList.addAll(getSelectExprColumnStrList(selectExpressionItems.get(i).getExpression()));
        }
        this.innerNode.initProjPushDownInfo();

    }
}