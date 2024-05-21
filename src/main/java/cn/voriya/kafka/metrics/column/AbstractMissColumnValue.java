package cn.voriya.kafka.metrics.column;

import scala.Function0;

public abstract class AbstractMissColumnValue<R> implements Function0<R> {
    public AbstractMissColumnValue() {
        setValue();
    }
    public R VALUE;

    protected abstract void setValue();

    public R apply() {
        return VALUE;
    }

    @Override
    public boolean apply$mcZ$sp() {
        return false;
    }

    @Override
    public byte apply$mcB$sp() {
        return 0;
    }

    @Override
    public char apply$mcC$sp() {
        return 0;
    }

    @Override
    public double apply$mcD$sp() {
        return 0;
    }

    @Override
    public float apply$mcF$sp() {
        return 0;
    }

    @Override
    public int apply$mcI$sp() {
        return 0;
    }

    @Override
    public long apply$mcJ$sp() {
        return 0;
    }

    @Override
    public short apply$mcS$sp() {
        return 0;
    }

    @Override
    public void apply$mcV$sp() {

    }
}

