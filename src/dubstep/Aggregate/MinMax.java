package dubstep.Aggregate;

import dubstep.utils.Evaluator;
import dubstep.utils.Tuple;
import net.sf.jsqlparser.expression.*;

import java.sql.SQLException;

public class MinMax extends Aggregate {

    private boolean isMin;
    private PrimitiveValue result;

    public MinMax (Expression expression, Evaluator evaluator, boolean isMin) {
        super(expression, evaluator);
        this.isMin = isMin;
    }

    @Override
    public void resetCurrentResult() {
        result = null;
    }

    @Override
    public PrimitiveValue yield (Tuple tuple) {
        this.evaluator.setTuple(tuple);
        PrimitiveValue curResult = null;

        try {
            curResult = evaluator.eval(expression);
            if (isMin) {
                if (result == null) {
                    result = curResult;
                } else if (curResult instanceof LongValue) {
                    if (curResult.toLong() < result.toLong()) result = curResult;
                } else if (curResult instanceof DoubleValue) {
                    if (curResult.toDouble() < result.toDouble()) result = curResult;
                } else if (curResult instanceof StringValue) {
                    if (result.toString().compareTo(curResult.toString()) > 0) result = curResult;
                } else if (curResult instanceof DateValue) {
                    if (((DateValue) result).getValue().getTime() > ((DateValue) curResult).getValue().getTime()) result = curResult;
                }
            }else {
                if (result == null) {
                    result = curResult;
                } else if (curResult instanceof LongValue) {
                    if (curResult.toLong() > result.toLong()) result = curResult;
                } else if (curResult instanceof DoubleValue) {
                    if (curResult.toDouble() > result.toDouble()) result = curResult;
                } else if (curResult instanceof StringValue) {
                    if (result.toString().compareTo(curResult.toString()) < 0) result = curResult;
                } else if (curResult instanceof DateValue) {
                    if (((DateValue) result).getValue().getTime() < ((DateValue) curResult).getValue().getTime()) result = curResult;
                }
            }
        }catch (SQLException e) {
            e.printStackTrace();
        }
        return null;
    }
}
