package dubstep.Aggregate;

import dubstep.utils.Evaluator;
import dubstep.utils.Tuple;
import net.sf.jsqlparser.expression.Expression;
import net.sf.jsqlparser.expression.Function;
import net.sf.jsqlparser.expression.PrimitiveValue;

public abstract class Aggregate {
    public PrimitiveValue result = null;
    public Expression expression;
    public Evaluator evaluator;

    protected Aggregate(Expression expression, Evaluator evaluator){
        this.expression = expression;
        this.evaluator = evaluator;
    }

    public abstract PrimitiveValue yield(Tuple tuple);

    public abstract void resetCurrentResult();

    public PrimitiveValue getCurrentResult() {
        return result;
    }

    public static Aggregate getAggObject(Function func, Evaluator evaluator){
        Expression expression = null;

        if (!func.isAllColumns()){
            expression = func.getParameters().getExpressions().get(0);
        }

        String funcName = func.getName().toLowerCase();

        if (funcName.equals("count")) return new Count(expression, evaluator);
        else if (funcName.equals("sum")) return new Sum(expression, evaluator);
        else if (funcName.equals("avg")) return new Average(expression, evaluator);
        else return null;
    }
}
