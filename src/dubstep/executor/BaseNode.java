package dubstep.executor;

import dubstep.utils.QueryTimer;
import dubstep.utils.Tuple;

import java.util.ArrayList;

import static dubstep.Main.EXPLAIN_MODE;

abstract public class BaseNode {
    public BaseNode innerNode, outerNode;  //Inner node is used for every node - outer node is used for join
    NodeType type;
    QueryTimer timer; //user for probing running time for every node
    Integer tupleCount;
    ArrayList<String> projectionInfo;

    BaseNode() {
        timer = new QueryTimer();
        timer.reset();
        tupleCount = 0;
    }

    abstract Tuple getNextRow();

    abstract void resetIterator();

    abstract void InitProjectionInfo();

    public Tuple getNextTuple() {
        if (EXPLAIN_MODE)
            timer.start();

        Tuple nextRow = this.getNextRow();


        if (EXPLAIN_MODE) {
            tupleCount++;
            timer.stop();
        }

        return nextRow;

    }

    enum NodeType {
        SORT_NODE, PROJ_NODE, SELECT_NODE, SCAN_NODE
    }


}


