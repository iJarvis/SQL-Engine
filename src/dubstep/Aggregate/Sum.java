package dubstep.Aggregate;

import dubstep.utils.Evaluator;
import dubstep.utils.Tuple;
import net.sf.jsqlparser.expression.DoubleValue;
import net.sf.jsqlparser.expression.Expression;
import net.sf.jsqlparser.expression.LongValue;
import net.sf.jsqlparser.expression.PrimitiveValue;

import java.sql.SQLException;

public class Sum extends Aggregate {
    public LongValue longValue = new LongValue(0);
    public DoubleValue doubleValue = new DoubleValue(0);
    public Boolean isLong = null;

    public Sum(Expression expression, Evaluator evaluator) {
        super(expression, evaluator);
    }

    @Override
    public void resetCurrentResult() {
        this.longValue.setValue(0);
        this.doubleValue.setValue(0);
        this.isLong = null;
    }

    @Override
    public PrimitiveValue yield(Tuple tuple) {
        PrimitiveValue result = null;
        evaluator.setTuple(tuple);

        try {
            result = evaluator.eval(expression);
            if (isLong == null) isLong = result instanceof LongValue;
            if (isLong) {
                longValue.setValue(longValue.getValue() + result.toLong());
                this.result = longValue;
                return longValue;
            } else {
                doubleValue.setValue(doubleValue.getValue() + result.toDouble());
                this.result = doubleValue;
                return doubleValue;
            }
        } catch (SQLException e) {
            e.printStackTrace();
        }

        this.result = longValue;

        return result;
    }
}
