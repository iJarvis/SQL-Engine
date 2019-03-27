package dubstep.executor;

import dubstep.utils.Evaluator;
import dubstep.utils.Tuple;
import net.sf.jsqlparser.expression.Expression;
import net.sf.jsqlparser.expression.PrimitiveValue;

import java.sql.SQLException;

public class SelectNode extends BaseNode {

    public Expression filter;
    Eval eval;

    public SelectNode(Expression filter, BaseNode InnerNode) {
        this.filter = filter;
        this.innerNode = InnerNode;
        innerNode.parentNode = this;
        eval = new Evaluator(this.innerNode.projectionInfo);
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
                eval.setTuple(row);
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
