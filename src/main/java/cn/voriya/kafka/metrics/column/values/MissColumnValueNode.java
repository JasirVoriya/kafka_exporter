package cn.voriya.kafka.metrics.column.values;

import cn.voriya.kafka.metrics.column.AbstractMissColumnValue;
import org.apache.kafka.common.Node;

public class MissColumnValueNode extends AbstractMissColumnValue<Node> {
    @Override
    protected void setValue() {
        VALUE = new Node(-1, "Not found", -1);
    }
}
