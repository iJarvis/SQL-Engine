package dubstep.executor;

import dubstep.utils.Tuple;
import dubstep.utils.TupleComparator;
import dubstep.utils.Utils;
import net.sf.jsqlparser.expression.PrimitiveValue;
import net.sf.jsqlparser.schema.Column;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;

import static dubstep.planner.PlanTree.getSelectExprColumnList;

public class SortMergeJoinNode extends BaseNode {

    public Column innerColumn;
    public Column outerColumn;
    private Tuple innerTuple = null;
    private Tuple outerTuple = null;
    private boolean initDone = false;
    private List<Tuple> currentOuterRange = new ArrayList<>();
    private int outerRangeIdx = 0;

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
        if (!initDone) {
            innerTuple = innerNode.getNextTuple();
            outerTuple = outerNode.getNextTuple();
            if (innerTuple == null || outerTuple == null) {
                return null;
            }
            initDone = true;
        }

        if (currentOuterRange.size() != 0) {
            if (outerRangeIdx < currentOuterRange.size()) {
                return new Tuple(innerTuple, currentOuterRange.get(outerRangeIdx++));
            } else {
                innerTuple = innerNode.getNextTuple();
                if (innerTuple == null) return null;
                PrimitiveValue innerPV = innerTuple.getValue(innerColumn, innerNode.projectionInfo);
                PrimitiveValue outerPV = currentOuterRange.get(0).getValue(outerColumn, outerNode.projectionInfo);
                if (TupleComparator.compare(innerPV, outerPV) == 0) {
                    outerRangeIdx = 0;
                    return new Tuple(innerTuple, currentOuterRange.get(outerRangeIdx++));
                }
            }
        }
        if(outerTuple == null)
            return null;

        PrimitiveValue innerPV = innerTuple.getValue(innerColumn, innerNode.projectionInfo);
        PrimitiveValue outerPV = outerTuple.getValue(outerColumn, outerNode.projectionInfo);

        while (true) {
            while (TupleComparator.compare(innerPV, outerPV) < 0) {
                innerTuple = innerNode.getNextTuple();
                if (innerTuple == null) break;
                innerPV = innerTuple.getValue(innerColumn, innerNode.projectionInfo);
            }

            while (TupleComparator.compare(innerPV, outerPV) > 0) {
                outerTuple = outerNode.getNextTuple();
                if (outerTuple == null) break;
                outerPV = outerTuple.getValue(outerColumn, outerNode.projectionInfo);
            }

            if (innerTuple == null || outerTuple == null) {
                return null;
            }

            if (TupleComparator.compare(innerPV, outerPV) == 0) break;
        }

        currentOuterRange.clear();
        outerRangeIdx = 0;
        while (outerTuple != null && TupleComparator.compare(outerTuple.getValue(outerColumn, outerNode.projectionInfo), outerPV) == 0) {
            currentOuterRange.add(outerTuple);
            outerTuple = outerNode.getNextTuple();
        }

        return new Tuple(innerTuple, currentOuterRange.get(outerRangeIdx++));
    }

    @Override
    void resetIterator() {
        innerNode.resetIterator();
        outerNode.resetIterator();
        innerTuple = null;
        outerTuple = null;
        outerRangeIdx = 0;
        currentOuterRange.clear();
        initDone = false;
    }

    @Override
    void initProjectionInfo() {
        projectionInfo = new HashMap<>(innerNode.projectionInfo);
        Utils.mapPutAll(outerNode.projectionInfo, projectionInfo);
        typeList = new ArrayList<>(innerNode.typeList);
        typeList.addAll(outerNode.typeList);
    }

    @Override
    public void initProjPushDownInfo() {
        this.requiredList.addAll(this.parentNode.requiredList);
        this.requiredList.add(innerColumn.getWholeColumnName());
        this.requiredList.add(outerColumn.getWholeColumnName());
        this.innerNode.initProjPushDownInfo();
        this.outerNode.initProjPushDownInfo();
    }
}
