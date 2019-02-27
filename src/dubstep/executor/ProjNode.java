package dubstep.executor;

import dubstep.planner.PlanTree;
import dubstep.utils.Logger;
import dubstep.utils.Tuple;
import net.sf.jsqlparser.eval.Eval;
import net.sf.jsqlparser.expression.PrimitiveValue;
import net.sf.jsqlparser.schema.Column;
import net.sf.jsqlparser.statement.select.AllTableColumns;
import net.sf.jsqlparser.statement.select.SelectExpressionItem;
import net.sf.jsqlparser.statement.select.SelectItem;

import java.sql.SQLException;
import java.util.ArrayList;
import java.util.List;

public class ProjNode extends BaseNode {

    private List<SelectItem> selectItems; //used for building of projection info
    private List<SelectExpressionItem> selectExpressionItems = new ArrayList<>();
    private List<String> completeProjectionTables = new ArrayList<>();

    public ProjNode(List<SelectItem> selectItems, BaseNode InnerNode) {
        super();
        this.innerNode = InnerNode;
        this.selectItems = selectItems;

        for (SelectItem item : selectItems) {
            if (item instanceof AllTableColumns) {
                completeProjectionTables.add(((AllTableColumns) item).getTable().getName());
            } else if (item instanceof SelectExpressionItem) {
                selectExpressionItems.add((SelectExpressionItem) item);
            } else {
                throw new UnsupportedOperationException("We don't support this select operation - " + item.toString());
            }
        }

        initProjectionInfo();
        for (String column: projectionInfo) {
            Logger.logd(column);
        }
    }

    @Override
    Tuple getNextRow() {
        List<PrimitiveValue> values = new ArrayList<>();
        Tuple nextRow = this.innerNode.getNextRow();
        if (nextRow == null)
            return null;
        Eval eval = new Eval() {
            @Override
            public PrimitiveValue eval(Column column) {
                return nextRow.getValue(column, projectionInfo);
            }
        };
        for (SelectExpressionItem expression : this.selectExpressionItems) {
            try {
                PrimitiveValue value = eval.eval(expression.getExpression());
                values.add(value);
            } catch (SQLException e) {
                e.printStackTrace();
            }
        }
        //hack for now. hate me.
        if (completeProjectionTables.size() != 0) {
            for (String column: projectionInfo) {
                PrimitiveValue value = nextRow.getValue(column, column, projectionInfo);
                values.add(value);
            }
        }

        return new Tuple(values);
    }

    @Override
    void resetIterator() {
        innerNode.resetIterator();
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
        if (completeProjectionTables.size() != 0) {
            for (String column : innerNode.projectionInfo) {
                for (String table: completeProjectionTables) {
                    if (column.startsWith(table)) {
                        projectionInfo.add(column);
                    }
                }
            }
        }
    }
}
