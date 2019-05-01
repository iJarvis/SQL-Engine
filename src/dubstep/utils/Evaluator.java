package dubstep.utils;

import net.sf.jsqlparser.eval.Eval;
import net.sf.jsqlparser.expression.PrimitiveValue;
import net.sf.jsqlparser.schema.Column;

import java.util.ArrayList;
import java.util.Map;

public class Evaluator extends Eval {
    private Tuple tuple;
    public Map<String, Integer> projectionInfo;
    ArrayList<Integer> requestList = new ArrayList<>();
    Integer currentIndex = 0;
    public Boolean safeMode = true;

    public Evaluator(Map<String, Integer> projectionInfo){
        super();
        this.projectionInfo = projectionInfo;
    }

    public PrimitiveValue eval(Column column) {
        if(!safeMode)
            return this.tuple.getValue(column,projectionInfo);
        if(requestList.size() > currentIndex ) {
            PrimitiveValue value = this.tuple.getValue(requestList.get(currentIndex));
            currentIndex++;
            return value;
        }
        else
        {
            requestList.add(this.tuple.GetPosition(column,projectionInfo));
            PrimitiveValue value = this.tuple.getValue(requestList.get(currentIndex));
            currentIndex++;
            return value;
        }


    }

    public void setTuple(Tuple tuple){
        this.currentIndex = 0;
        this.tuple = tuple;
    }

}
