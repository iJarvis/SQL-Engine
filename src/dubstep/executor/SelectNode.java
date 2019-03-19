package dubstep.executor;

import dubstep.utils.Logger;
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
        while(true) {
            Tuple row = this.innerNode.getNextTuple();
            if(row == null)
                return  null;
            if (filter == null)
                return row;
            else {
                Eval eval = new Eval() {
                    @Override
                    public PrimitiveValue eval(Column column) {
                        return row.getValue(column, innerNode.projectionInfo);
                    }
                };
                try {
                    PrimitiveValue value = eval.eval(filter);
                    if (value.toBool()) {
                        return row;
                    }
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
