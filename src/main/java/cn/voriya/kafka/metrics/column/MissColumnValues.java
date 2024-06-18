package cn.voriya.kafka.metrics.column;

import cn.voriya.kafka.metrics.column.values.MissColumnValueInteger;
import cn.voriya.kafka.metrics.column.values.MissColumnValueLong;
import cn.voriya.kafka.metrics.column.values.MissColumnValueNode;
import cn.voriya.kafka.metrics.column.values.MissColumnValueString;
import lombok.AccessLevel;
import lombok.NoArgsConstructor;

@NoArgsConstructor(access = AccessLevel.MODULE)
public class MissColumnValues {
    public static final MissColumnValueInteger INTEGER = new MissColumnValueInteger();
    public static final MissColumnValueLong LONG = new MissColumnValueLong();
    public static final MissColumnValueString STRING = new MissColumnValueString();
    public static final MissColumnValueNode NODE = new MissColumnValueNode();
}
