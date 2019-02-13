package dubstep.executor;

import dubstep.utils.queryTimer;
import dubstep.utils.tuple;

import static dubstep.Main.explain_mode;

abstract class baseNode {
    node_type type;
    queryTimer timer; //user for probing running time for every node
    baseNode innerNode,outerNode;  //Inner node is used for every node - outer node is used for join
    Integer tupleCount; //

    protected abstract tuple GetNextRow();

    abstract void ResetIterator();

    baseNode()
    {
        timer = new queryTimer();
        timer.reset();
        tupleCount = 0;
    }

    tuple GetNextTuple()
    {
        if(explain_mode)
            timer.start();

        tuple nextRow = this.GetNextRow();


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


