package dubstep.executor;

import dubstep.utils.Tuple;
import dubstep.utils.TupleComparator;
import net.sf.jsqlparser.expression.PrimitiveValue;
import net.sf.jsqlparser.schema.Column;

import java.util.ArrayList;

public class SortMergeJoinNode extends BaseNode {

    private Column innerColumn;
    private Column outerColumn;
    private Tuple innerTuple = null;
    private Tuple outerTuple = null;

    public SortMergeJoinNode(BaseNode innerNode, BaseNode outerNode, Column innerColumn, Column outerColumn) {
        super();
        this.innerNode = innerNode;
        this.outerNode = outerNode;
        this.innerColumn = innerColumn;
        this.outerColumn = outerColumn;
        initProjectionInfo();
    }

    @Override
    Tuple getNextRow() {

        innerTuple = innerNode.getNextTuple();
        outerTuple = outerNode.getNextTuple();

        if (innerTuple == null || outerTuple == null) {
            return null;
        }

        PrimitiveValue innerPV = innerTuple.getValue(innerColumn, innerNode.projectionInfo);
        PrimitiveValue outerPV = outerTuple.getValue(outerColumn, outerNode.projectionInfo);

        while (TupleComparator.compare(innerPV, outerPV, true) < 0) {
            innerTuple = innerNode.getNextTuple();
            if (innerTuple == null) break;
            innerPV = innerTuple.getValue(innerColumn, innerNode.projectionInfo);
        }

        while (TupleComparator.compare(innerPV, outerPV, true) > 0) {
            outerTuple = outerNode.getNextTuple();
            if (outerTuple == null) break;
            outerPV = outerTuple.getValue(outerColumn, outerNode.projectionInfo);
        }

        if (innerTuple == null || outerTuple == null) {
            return null;
        }

        int compareResult = TupleComparator.compare(innerPV, outerPV, true);
        if (compareResult == 0) {
            return new Tuple(innerTuple, outerTuple);
        }

        return null;
    }

    @Override
    void resetIterator() {
        innerNode.resetIterator();
        outerNode.resetIterator();
        innerTuple = null;
    }

    @Override
    void initProjectionInfo() {
        this.projectionInfo = new ArrayList<>(innerNode.projectionInfo);
        this.projectionInfo.addAll(outerNode.projectionInfo);
    }
}
