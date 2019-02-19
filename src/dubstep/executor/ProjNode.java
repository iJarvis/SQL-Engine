package dubstep.executor;

import dubstep.utils.Tuple;
import javafx.util.Pair;
import net.sf.jsqlparser.statement.select.SelectExpressionItem;
import net.sf.jsqlparser.statement.select.SelectItem;

import java.util.ArrayList;
import java.util.List;

public class ProjNode extends BaseNode {


    ArrayList<Integer> ProjectionVector;
    Boolean IsCompleteProjection;
    List<SelectItem> SelectItems;


    public ProjNode( List<SelectItem> selectItems,BaseNode InnerNode) {
        super();
        for (SelectItem item : selectItems) {
            if (!(item instanceof SelectExpressionItem)) {
                if(item.toString().equals("*"))
                    this.IsCompleteProjection = true;
                else
                    throw new UnsupportedOperationException("We don't support this column type yet");
            }
            else
            {
                throw new UnsupportedOperationException("We are yet to support selective projection");

            }
        }
        this.innerNode = InnerNode;

        InitProjectionInfo();


    }

    @Override
    Tuple getNextRow() {
        Tuple nextRow = this.innerNode.getNextRow();

        if(IsCompleteProjection)
            return nextRow;
        else
            return new Tuple(nextRow,ProjectionVector);


    }

    @Override
    void resetIterator() {

    }

    @Override
    void InitProjectionInfo() {
        if(IsCompleteProjection)
            this.ProjectionInfo = this.innerNode.ProjectionInfo;
        else
        {
            ProjectionInfo = new ArrayList<>();
            for(Object selectItem : this.SelectItems)
            {
                ProjectionInfo.add(selectItem.toString());
            }
        }

    }


}
