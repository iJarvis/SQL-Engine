package dubstep.executor;

import dubstep.utils.Evaluator;
import dubstep.utils.Tuple;
import net.sf.jsqlparser.expression.BinaryExpression;
import net.sf.jsqlparser.expression.Expression;
import net.sf.jsqlparser.expression.PrimitiveValue;
import net.sf.jsqlparser.schema.Column;

import java.sql.SQLException;
import java.util.ArrayList;

import static dubstep.planner.PlanTree.getSelectExprColumnList;
import static dubstep.planner.PlanTree.getSelectExprColumnStrList;

public class SelectNode extends BaseNode {

    public Expression filter;
    public Evaluator eval;
    public boolean isOptimized = false;
    Integer Index;
    Boolean isBinarySingular = false;
    Column singname;

    public SelectNode(Expression filter, BaseNode InnerNode) {
        super();
        this.filter = filter;
        this.innerNode = InnerNode;
        innerNode.parentNode = this;
        eval = new Evaluator(this.innerNode.projectionInfo);

        if(filter instanceof BinaryExpression)
        {
            BinaryExpression be = (BinaryExpression)filter;
            if(be.getRightExpression() instanceof PrimitiveValue && be.getLeftExpression()  instanceof Column)
            {
                isBinarySingular = true;
                singname = ((Column) be.getLeftExpression());
            }
        }
        this.initProjectionInfo();
    }

    @Override
    Tuple getNextRow() {
        while(true) {
            Tuple row = this.innerNode.getNextTuple();
            if(row == null)
                return  null;
            if (filter == null)
                return row;
            if (Index == null && isBinarySingular)
            {
                Index = row.GetPosition(singname,this.projectionInfo);
            }
            if(isBinarySingular)
            {

                ((BinaryExpression)filter).setLeftExpression(row.valueArray[Index]);
            }
                eval.setTuple(row);
                try {
                    PrimitiveValue value = eval.eval(filter);
                    if (value!= null && value.toBool()) {
                        return row;
                    }
                } catch (SQLException e) {
                    e.printStackTrace();
                }
        }
    }

    @Override
    void resetIterator() {
        innerNode.resetIterator();
    }

    @Override
    void initProjectionInfo() {
        projectionInfo = innerNode.projectionInfo;
    }

    @Override
    public void initProjPushDownInfo() {
        requiredList.addAll(parentNode.requiredList);
        requiredList.addAll((ArrayList)getSelectExprColumnStrList(filter));
        this.innerNode.initProjPushDownInfo();
    }
}
