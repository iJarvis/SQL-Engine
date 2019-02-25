package dubstep.executor;

import dubstep.utils.Tuple;
import net.sf.jsqlparser.eval.Eval;
import net.sf.jsqlparser.expression.PrimitiveValue;
import net.sf.jsqlparser.schema.Column;
import net.sf.jsqlparser.statement.select.SelectExpressionItem;
import net.sf.jsqlparser.statement.select.SelectItem;

import java.sql.SQLException;
import java.util.ArrayList;
import java.util.List;

public class ProjNode extends BaseNode {

    private List<Integer> projectionVector = new ArrayList<>();
    private boolean isCompleteProjection; // handle * cases
    private List<SelectItem> selectItems; //used for building of projection info
    private List<SelectExpressionItem> selectExpressionItems = new ArrayList<>();

    public ProjNode(List<SelectItem> selectItems, BaseNode InnerNode) {
        super();
        this.innerNode = InnerNode;
        this.selectItems = selectItems;

        for (SelectItem item : selectItems) {
            if (!(item instanceof SelectExpressionItem)) {
                // case select * from table
                if (item.toString().equals("*"))
                    this.isCompleteProjection = true;
                    throw new UnsupportedOperationException("We don't support this column type yet");
            }
            // All other projection cases
            else {
                selectExpressionItems.add((SelectExpressionItem)item);

            }
        }

        initProjectionInfo();
    }

    @Override
    Tuple getNextRow() {
        List<PrimitiveValue> values = new ArrayList<>();
        Tuple nextRow = this.innerNode.getNextRow();
        if (nextRow == null)
            return null;
        Eval eval = new Eval() {
            @Override
            public PrimitiveValue eval(Column column) throws SQLException {
                return nextRow.GetValue(column, innerNode.projectionInfo);
            }
        };
        for(SelectExpressionItem expression : this.selectExpressionItems)
        {
            try {

                PrimitiveValue value = eval.eval(expression.getExpression());
                values.add(value);
            } catch (SQLException e) {
                e.printStackTrace();
            }
        }

        if (isCompleteProjection)
            return nextRow;
        else
            return new Tuple(values);
    }

    @Override
    void resetIterator() {

    }

    @Override
    void initProjectionInfo() {
        if (isCompleteProjection)
            this.projectionInfo = this.innerNode.projectionInfo;
        else {
            projectionInfo = new ArrayList<>();
            for (SelectItem selectItem : this.selectItems) {
                String columnName = ((SelectExpressionItem) selectItem).getExpression().toString();
                String alias = ((SelectExpressionItem) selectItem).getAlias();
                if(alias == null)
                projectionInfo.add(columnName);
                else
                    projectionInfo.add(alias);
            }
        }
    }
}
