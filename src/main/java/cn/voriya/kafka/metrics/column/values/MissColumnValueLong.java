package cn.voriya.kafka.metrics.column.values;

import cn.voriya.kafka.metrics.column.AbstractMissColumnValue;

public class MissColumnValueLong extends AbstractMissColumnValue<Long> {

    @Override
    protected void setValue() {
        VALUE = 0L;
    }
}
