package cn.voriya.kafka.metrics.column.values;

import cn.voriya.kafka.metrics.column.AbstractMissColumnValue;

public class MissColumnValueInteger extends AbstractMissColumnValue<Integer> {
    @Override
    protected void setValue() {
        VALUE = 0;
    }
}
