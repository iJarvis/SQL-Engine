package dubstep.Aggregate;

import dubstep.utils.Evaluator;
import dubstep.utils.Tuple;
import net.sf.jsqlparser.expression.DoubleValue;
import net.sf.jsqlparser.expression.Expression;
import net.sf.jsqlparser.expression.LongValue;
import net.sf.jsqlparser.expression.PrimitiveValue;

public class Average extends Sum {
    private Count counter;

    public Average(Expression expression, Evaluator evaluator) {
        super(expression, evaluator);
        counter = new Count(expression, evaluator);
    }

    @Override
    public void resetCurrentResult() {
        this.counter.resetCurrentResult();
        super.resetCurrentResult();
    }

    @Override
    public PrimitiveValue yield(Tuple tuple) {
        super.yield(tuple);

        long count = 1;

        try {
            count = counter.yield(tuple).toLong();
        } catch (PrimitiveValue.InvalidPrimitive e) {
            e.printStackTrace();
        }

        if (isLong) {
            LongValue result = new LongValue(longValue.getValue());
            result.setValue(result.getValue() / count);
            this.result = result;
            return result;
        } else {
            DoubleValue result = new DoubleValue(doubleValue.getValue());
            result.setValue(result.getValue() / count);
            this.result = result;
            return result;
        }

    }
}
