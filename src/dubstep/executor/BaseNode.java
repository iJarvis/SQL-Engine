package dubstep.executor;

import dubstep.utils.QueryTimer;
import dubstep.utils.Tuple;

import java.util.ArrayList;

import static dubstep.Main.EXPLAIN_MODE;

abstract public class BaseNode {
    public BaseNode innerNode, outerNode, parentNode;  //Inner node is used for every node - outer node is used for join

    NodeType type;
    QueryTimer timer; //user for probing running time for every node
    Integer tupleCount;
    public ArrayList<String> projectionInfo;
    public Boolean isInner;

    BaseNode() {
        timer = new QueryTimer();
        timer.reset();
        tupleCount = 0;
        isInner = true;
    }

    abstract Tuple getNextRow();

    abstract void resetIterator();

    abstract void initProjectionInfo();

    public Integer getTupleCount(){
        return this.tupleCount;
    }

    public Long getExecutionTime(){
        return this.timer.getTotalTime();
    }

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

    enum DataType{
        NONE, LONG, DOUBLE, DATE, STRING
    }
}


