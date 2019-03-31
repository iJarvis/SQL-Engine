package dubstep.Aggregate;

import dubstep.utils.Evaluator;
import dubstep.utils.Tuple;
import net.sf.jsqlparser.expression.*;

import java.sql.SQLException;

public class Max extends Aggregate {

    private PrimitiveValue result;

    public Max (Expression expression, Evaluator evaluator) {
        super(expression, evaluator);
        this.result = null;
    }

    @Override
    public void resetCurrentResult() {
        result = null;
    }

    @Override
    public PrimitiveValue yield (Tuple tuple) {
        this.evaluator.setTuple(tuple);
        PrimitiveValue curResult;

        try {
            curResult = evaluator.eval(expression);

            if (result == null) {
                result = curResult;
                System.out.println("BOOM");
            } else if (curResult instanceof LongValue) {
                if (curResult.toLong() > result.toLong()) result = curResult;
            } else if (curResult instanceof DoubleValue) {
                if (curResult.toDouble() > result.toDouble()) result = curResult;
            } else if (curResult instanceof StringValue) {
                if (result.toString().compareTo(curResult.toString()) < 0) result = curResult;
            } else if (curResult instanceof DateValue) {
                if (((DateValue) result).getValue().getTime() < ((DateValue) curResult).getValue().getTime()) result = curResult;
            }

        }catch (SQLException e) {
            e.printStackTrace();
        }
        return result;
    }
}
