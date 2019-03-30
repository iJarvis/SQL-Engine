package dubstep.utils;

import dubstep.executor.AggNode;
import dubstep.executor.BaseNode;
import dubstep.executor.GroupByNode;
import dubstep.executor.ProjNode;
import net.sf.jsqlparser.expression.Expression;
import net.sf.jsqlparser.expression.Function;
import net.sf.jsqlparser.schema.Column;
import net.sf.jsqlparser.statement.select.AllTableColumns;
import net.sf.jsqlparser.statement.select.SelectExpressionItem;
import net.sf.jsqlparser.statement.select.SelectItem;

import java.util.ArrayList;
import java.util.List;

// Returns ProjNode/AggNode/GroupByNode based on requirements
public class GenerateAggregateNode {
    BaseNode inner;
    private List<SelectItem> selectItems;
    private ArrayList<SelectExpressionItem> selectExpressionItems = new ArrayList<>();
    private List<String> completeProjectionTables = new ArrayList<>();

    public GenerateAggregateNode(List<SelectItem> selectItems, BaseNode innerNode) {
        this.inner = innerNode;
        this.selectItems = selectItems;
        this.inner = null;

        for (SelectItem item : selectItems) {
            if (item instanceof AllTableColumns) {
                this.inner = new ProjNode(this.selectItems, this.inner);
            } else if (item instanceof SelectExpressionItem) {
                selectExpressionItems.add((SelectExpressionItem) item);
            } else {
                throw new UnsupportedOperationException("We don't support this select operation - " + item.toString());
            }
        }

        ArrayList<Expression> selectExpressions = new ArrayList<>();

        for (SelectExpressionItem expressionItems : selectExpressionItems) {
            selectExpressions.add(expressionItems.getExpression());
        }

        boolean hasColumns = false;
        boolean hasFunctions = false;

        for (Expression expression : selectExpressions) {
            if (expression instanceof Column)
                hasColumns = true;
            if (expression instanceof Function)
                hasFunctions = true;
        }

        if (hasColumns && hasFunctions)
            inner = new GroupByNode(innerNode, selectExpressionItems);
        else if (hasFunctions)
            inner = new AggNode(innerNode, selectExpressionItems);
        else
            inner = new ProjNode(selectItems, innerNode);
    }

    public BaseNode getAggregateNode() {
        return this.inner;
    }
}
