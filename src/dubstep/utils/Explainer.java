package dubstep.utils;

import dubstep.executor.*;

public class Explainer {

    BaseNode root;

    public Explainer(BaseNode root) {
        this.root = root;
    }

    public void explainTree(BaseNode root, int indentLevel) {

        if (root == null)
            return;

        String explainString = root.getClass().getSimpleName();

        if (root instanceof ScanNode)
            explainString += ", Table scanned : " + ((ScanNode) root).getScanTableName();

        explainString += ", Number of tuples : " + root.getTupleCount() +
                ", Time taken : " + root.getExecutionTime() + " ms";
        if (root instanceof SelectNode)
            explainString += " filter = " + (((SelectNode) root).filter.toString());
        if (root instanceof HashJoinNode)
            explainString += " filter =" + (((HashJoinNode) root).filter.toString());
        if (root instanceof SortMergeJoinNode)
            explainString += " filter = " + (((SortMergeJoinNode) root).innerColumn.toString() + " = " + ((SortMergeJoinNode) root).outerColumn.toString());
        if (root instanceof ScanNode)
            explainString += " Parse time =" + ((ScanNode) root).parsetimer.getTotalTime();
        for (int i = 0; i < indentLevel; i++) {
            System.out.print("\t|");
        }

        System.out.println(explainString);

        explainTree(root.innerNode, indentLevel + 1);
        explainTree(root.outerNode, indentLevel + 1);
    }

    public void explain() {

        explainTree(this.root, 0);

    }

}
