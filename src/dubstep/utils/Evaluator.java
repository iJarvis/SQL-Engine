package dubstep.utils;

import net.sf.jsqlparser.eval.Eval;
import net.sf.jsqlparser.expression.PrimitiveValue;
import net.sf.jsqlparser.schema.Column;

import java.util.Map;

public class Evaluator extends Eval {
    private Tuple tuple;
    Map<String, Integer> projectionInfo;

    public Evaluator(Map<String, Integer> projectionInfo){
        super();
        this.projectionInfo = projectionInfo;
    }

    public PrimitiveValue eval(Column column) {
        return this.tuple.getValue(column, projectionInfo);
    }

    public void setTuple(Tuple tuple){
        this.tuple = tuple;
    }

}
