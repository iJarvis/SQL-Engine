package dubstep.utils;

import dubstep.utils.Tuple;
import net.sf.jsqlparser.eval.Eval;
import net.sf.jsqlparser.expression.Function;
import net.sf.jsqlparser.expression.PrimitiveValue;
import net.sf.jsqlparser.schema.Column;
import net.sf.jsqlparser.schema.Table;

import java.util.ArrayList;

public class Evaluator extends Eval {
    private Tuple tuple;
    ArrayList<String> projectionInfo;

    public Evaluator(ArrayList<String> projectionInfo){
        super();
        this.projectionInfo = projectionInfo;
    }

    public PrimitiveValue eval(Column column) {
        return this.tuple.getValue(column, this.projectionInfo);
    }

    public void setTuple(Tuple tuple){
        this.tuple = tuple;
    }

}
