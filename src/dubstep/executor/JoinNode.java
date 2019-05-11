package dubstep.executor;

import dubstep.utils.Tuple;
import dubstep.utils.Utils;

import java.util.ArrayList;
import java.util.HashMap;

public class JoinNode extends BaseNode {

    private Tuple innerTuple;
    private boolean initJoin = false;

    public JoinNode(BaseNode innerNode, BaseNode outerNode) {
        this.innerNode = innerNode;
        this.innerNode.parentNode = this;
        this.outerNode = outerNode;
        this.outerNode.parentNode = this;
        this.outerNode.isInner = false;
        initProjectionInfo();
    }

    @Override
    Tuple getNextRow() {
        if (!initJoin) {
            innerTuple = innerNode.getNextTuple();
            initJoin = true;
        }
        if (innerTuple == null)
            return null;
        Tuple outerTuple;
        outerTuple = outerNode.getNextTuple();

        if (outerTuple != null) {
            return new Tuple(innerTuple, outerTuple);
        } else {
            this.outerNode.resetIterator();
            innerTuple = innerNode.getNextTuple();
            outerTuple = outerNode.getNextTuple();
            if (innerTuple == null || outerTuple == null) {
                return null;
            } else {
                return new Tuple(innerTuple, outerTuple);
            }
        }
    }

    @Override
    void resetIterator() {
        innerTuple = null;
        initJoin = false;
        innerNode.resetIterator();
        outerNode.resetIterator();
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
        this.requiredList = this.parentNode.requiredList;
        this.innerNode.initProjPushDownInfo();
        this.outerNode.initProjPushDownInfo();
    }
}
