package dubstep.executor;

import dubstep.utils.Tuple;
import net.sf.jsqlparser.expression.Expression;
import net.sf.jsqlparser.expression.PrimitiveValue;
import net.sf.jsqlparser.eval.*;
import net.sf.jsqlparser.schema.Column;

import java.sql.SQLException;

public class SelectNode extends BaseNode {

    private Expression filter;
    Eval eval;

    public SelectNode(Expression filter, BaseNode InnerNode) {
        this.filter = filter;
        this.innerNode = InnerNode;
        this.initProjectionInfo();
    }

    @Override
    Tuple getNextRow() {
        while(1 == 1) {
            Tuple row = this.innerNode.getNextRow();
            if(row == null)
                return  null;
            if (filter == null)
                return row;
            else {
                Eval eval = new Eval() {
                    @Override
                    public PrimitiveValue eval(Column column) throws SQLException {
                        return row.GetValue(column, innerNode.projectionInfo);
                    }
                };
                try {
                    PrimitiveValue value = eval.eval(filter);
                    if (value.toBool() == true)
                        return row;
                } catch (SQLException e) {
                    e.printStackTrace();
                }
            }
        }
    }

    @Override
    void resetIterator() {
        this.innerNode.resetIterator();
    }

    @Override
    void initProjectionInfo() {
        this.projectionInfo = this.innerNode.projectionInfo;
    }
}
