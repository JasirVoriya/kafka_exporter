package cn.voriya.kafka.metrics.column;

import lombok.AccessLevel;
import lombok.NoArgsConstructor;
import org.apache.kafka.common.Node;

class MissColumnValueInteger extends AbstractMissColumnValue<Integer> {
    @Override
    protected void setValue() {
        VALUE = 0;
    }
}

class MissColumnValueLong extends AbstractMissColumnValue<Long> {

    @Override
    protected void setValue() {
        VALUE = 0L;
    }
}

class MissColumnValueString extends AbstractMissColumnValue<String> {
    @Override
    protected void setValue() {
        VALUE = "-";
    }
}

class MissColumnValueNode extends AbstractMissColumnValue<Node> {
    @Override
    protected void setValue() {
        VALUE = new Node(-1, "Not found", -1);
    }
}

@NoArgsConstructor(access = AccessLevel.MODULE)
public class MissColumnValues {
    public static final MissColumnValueInteger INTEGER = new MissColumnValueInteger();
    public static final MissColumnValueLong LONG = new MissColumnValueLong();
    public static final MissColumnValueString STRING = new MissColumnValueString();
    public static final MissColumnValueNode NODE = new MissColumnValueNode();
}
