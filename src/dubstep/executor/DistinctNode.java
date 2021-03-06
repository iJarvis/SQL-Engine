package dubstep.executor;

import dubstep.utils.Tuple;
import net.sf.jsqlparser.schema.Column;
import net.sf.jsqlparser.statement.select.SelectExpressionItem;
import net.sf.jsqlparser.statement.select.SelectItem;

import java.util.HashSet;
import java.util.List;

public class DistinctNode extends BaseNode {
    private List<SelectExpressionItem> distinctItems;
    private HashSet<String> tuples;
    private Tuple tuple;

    public DistinctNode(List<SelectExpressionItem> distinctItems, BaseNode innerNode) {
        this.innerNode = innerNode;
        this.distinctItems = distinctItems;
        tuples = new HashSet<>();
        tuple = null;
        this.initProjectionInfo();
    }

    public Tuple getNextRow() {
        tuple = this.innerNode.getNextTuple();
        if (tuple == null) return null;
        String tupleString = "";
        for (SelectItem column : distinctItems) {
            tupleString = tupleString + tuple.getValue((Column) (((SelectExpressionItem) column).getExpression()), projectionInfo).toString();
        }
        while (tuples.contains(tupleString)) {
            tuple = this.innerNode.getNextTuple();
            if (tuple == null) return null;
        }
        tuples.add(tupleString);
        return tuple;
    }

    public void resetIterator() {
        this.tuples = null;
        this.innerNode.resetIterator();
    }

    public void initProjectionInfo() {
        projectionInfo = innerNode.projectionInfo;
        typeList = innerNode.typeList;
    }

    @Override
    public void initProjPushDownInfo() {
        if (this.parentNode != null)
            this.requiredList = this.parentNode.requiredList;
        this.innerNode.initProjPushDownInfo();
    }
}
