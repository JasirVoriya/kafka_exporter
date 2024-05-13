package cn.voriya.kafka.metrics.column;

import lombok.AccessLevel;
import lombok.NoArgsConstructor;
import lombok.SneakyThrows;

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

@NoArgsConstructor(access = AccessLevel.MODULE)
public class MissColumnValues {
    public static final MissColumnValueInteger INTEGER = new MissColumnValueInteger();
    public static final MissColumnValueLong LONG = new MissColumnValueLong();
    public static final MissColumnValueString STRING = new MissColumnValueString();

    public static <T> AbstractMissColumnValue<T> getDefault(Class<T> clazz) {
        return new AbstractMissColumnValue<T>() {
            @SneakyThrows
            @Override
            protected void setValue() {
                VALUE= clazz.newInstance();
            }
        };
    }
}
