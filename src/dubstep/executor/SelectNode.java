package dubstep.executor;

import dubstep.utils.Tuple;
import net.sf.jsqlparser.expression.Expression;

public class SelectNode extends BaseNode {

    private Expression filter;

    public SelectNode(Expression filter) {
        this.filter = filter;
    }

    @Override
    Tuple getNextRow() {
        return null;
    }

    @Override
    void resetIterator() {

    }
}
