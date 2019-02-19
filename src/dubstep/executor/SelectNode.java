package dubstep.executor;

import dubstep.utils.Tuple;
import net.sf.jsqlparser.expression.Expression;

public class SelectNode extends BaseNode {

    private Expression filter;

    public SelectNode(Expression filter,BaseNode InnerNode) {
        this.filter = filter;
        this.innerNode = InnerNode;
        this.InitProjectionInfo();
    }

    @Override
    Tuple getNextRow() {
        if(filter == null)
            return this.innerNode.getNextRow();
        else
            return null;
    }

    @Override
    void resetIterator() {
        this.innerNode.resetIterator();
    }
    @Override
    void InitProjectionInfo() {
        this.ProjectionInfo = this.innerNode.ProjectionInfo;

    }
}