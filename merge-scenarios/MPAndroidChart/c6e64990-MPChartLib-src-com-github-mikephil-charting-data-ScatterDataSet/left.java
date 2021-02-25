package com.github.mikephil.charting.data;

import android.graphics.Path;
import com.github.mikephil.charting.charts.ScatterChart.ScatterShape;
import com.github.mikephil.charting.interfaces.datasets.IScatterDataSet;
import com.github.mikephil.charting.utils.Utils;
import java.util.ArrayList;
import java.util.List;

public class ScatterDataSet extends LineScatterCandleRadarDataSet<Entry> implements IScatterDataSet {

    private float mShapeSize = 15f;

    private ScatterShape mScatterShape = ScatterShape.SQUARE;

    private Path mCustomScatterPath = null;

    public ScatterDataSet(List<Entry> yVals, String label) {
        super(yVals, label);
    }

    @Override
    public DataSet<Entry> copy() {
        List<Entry> yVals = new ArrayList<Entry>();
        for (int i = 0; i < mYVals.size(); i++) {
            yVals.add(mYVals.get(i).copy());
        }
        ScatterDataSet copied = new ScatterDataSet(yVals, getLabel());
        copied.mColors = mColors;
        copied.mShapeSize = mShapeSize;
        copied.mScatterShape = mScatterShape;
        copied.mCustomScatterPath = mCustomScatterPath;
        copied.mHighLightColor = mHighLightColor;
        return copied;
    }

    public void setScatterShapeSize(float size) {
        mShapeSize = Utils.convertDpToPixel(size);
    }

    @Override
    public float getScatterShapeSize() {
        return mShapeSize;
    }

    public void setScatterShape(ScatterShape shape) {
        mScatterShape = shape;
    }

    @Override
    public ScatterShape getScatterShape() {
        return mScatterShape;
    }

    public void setCustomScatterShape(Path shape) {
        mCustomScatterPath = shape;
    }

    public Path getCustomScatterShape() {
        return mCustomScatterPath;
    }
}