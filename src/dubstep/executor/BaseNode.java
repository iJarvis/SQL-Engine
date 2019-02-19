package dubstep.executor;

import dubstep.utils.QueryTimer;
import dubstep.utils.Tuple;

import java.util.ArrayList;
import java.util.HashMap;

import static dubstep.Main.EXPLAIN_MODE;

abstract public class BaseNode {
    NodeType type;
    QueryTimer timer; //user for probing running time for every node
    public BaseNode innerNode, outerNode;  //Inner node is used for every node - outer node is used for join
    Integer tupleCount;
    ArrayList<String> ProjectionInfo;

    abstract Tuple getNextRow();

    abstract void resetIterator();
    abstract void InitProjectionInfo();

    BaseNode() {
        timer = new QueryTimer();
        timer.reset();
        tupleCount = 0;
    }

    public Tuple getNextTuple()
    {
        if(EXPLAIN_MODE)
            timer.start();

        Tuple nextRow = this.getNextRow();


        if(EXPLAIN_MODE) {
            tupleCount++;
            timer.stop();
        }

        return  nextRow;

    }

    enum NodeType {
        SORT_NODE, PROJ_NODE, SELECT_NODE, SCAN_NODE
    }


}


