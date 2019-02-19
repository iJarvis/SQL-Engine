package dubstep.executor;

import dubstep.utils.Tuple;

import java.util.List;

public class ProjNode extends BaseNode {

    List<String> columns;

    public ProjNode(List<String> columns) {
        super();
        this.columns = columns;
    }

    @Override
    Tuple getNextRow() {
        return null;
    }

    @Override
    void resetIterator() {

    }
}
