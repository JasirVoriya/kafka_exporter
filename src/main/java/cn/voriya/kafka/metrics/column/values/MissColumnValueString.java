package cn.voriya.kafka.metrics.column.values;

import cn.voriya.kafka.metrics.column.AbstractMissColumnValue;

public class MissColumnValueString extends AbstractMissColumnValue<String> {
    @Override
    protected void setValue() {
        VALUE = "-";
    }
}
