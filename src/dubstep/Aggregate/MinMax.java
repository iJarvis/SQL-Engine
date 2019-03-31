package dubstep.Aggregate;

import dubstep.utils.Evaluator;
import dubstep.utils.Tuple;
import net.sf.jsqlparser.expression.DoubleValue;
import net.sf.jsqlparser.expression.Expression;
import net.sf.jsqlparser.expression.LongValue;
import net.sf.jsqlparser.expression.PrimitiveValue;

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
                }
            }
        }catch (SQLException e) {
            e.printStackTrace();
        }
        return null;
    }
}
