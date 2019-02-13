package dubstep.executor;

import dubstep.utils.QueryTimer;
import dubstep.utils.Tuple;

import static dubstep.Main.explain_mode;

abstract class BaseNode {
    node_type type;
    QueryTimer timer; //user for probing running time for every node
    BaseNode innerNode,outerNode;  //Inner node is used for every node - outer node is used for join
    Integer tupleCount; //

    abstract Tuple GetNextRow();

    abstract void ResetIterator();

    BaseNode()
    {
        timer = new QueryTimer();
        timer.reset();
        tupleCount = 0;
    }

    Tuple GetNextTuple()
    {
        if(explain_mode)
            timer.start();

        Tuple nextRow = this.GetNextRow();


        if(explain_mode) {
            tupleCount++;
            timer.stop();
        }

        return  nextRow;

    };

    enum node_type {
        SORT_NODE, PROJ_NODE, SELECT_NODE, SCAN_NODE
    }


}


