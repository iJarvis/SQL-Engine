package dubstep.utils;

import net.sf.jsqlparser.expression.Expression;
import net.sf.jsqlparser.expression.LongValue;
import net.sf.jsqlparser.expression.PrimitiveValue;

public class Count extends Aggregate{
    private LongValue count = new LongValue(0);

    public Count(Expression exp, Evaluator eval){
        super(exp,eval);
    }

    public PrimitiveValue yield(Tuple tuple){
        if(tuple == null)return null;
        count.setValue(count.getValue()+1);
        this.result = count;
        return count;
    }
}
