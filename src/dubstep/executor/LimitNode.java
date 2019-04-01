package dubstep.executor;

import dubstep.utils.Tuple;

public class LimitNode extends BaseNode {
    Long currentRowNo;
    Long maxRowNo;

    public LimitNode(Long limitValue, BaseNode innerNode) {
        currentRowNo = 0L;
        maxRowNo = limitValue;
        this.innerNode = innerNode;
        this.innerNode.parentNode = this;
        initProjectionInfo();
    }

    @Override
    Tuple getNextRow() {
        if (currentRowNo < maxRowNo) {
            currentRowNo++;
            return this.innerNode.getNextRow();
        } else
            return null;
    }

    @Override
    void initProjectionInfo() {
        this.projectionInfo = this.innerNode.projectionInfo;
    }

    @Override
    void resetIterator() {
        this.currentRowNo = 0L;
        this.innerNode.resetIterator();
    }
}