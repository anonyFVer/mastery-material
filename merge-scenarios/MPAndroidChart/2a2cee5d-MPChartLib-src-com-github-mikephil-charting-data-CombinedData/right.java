package com.github.mikephil.charting.data;

import com.github.mikephil.charting.highlight.Highlight;
import com.github.mikephil.charting.interfaces.datasets.IBarLineScatterCandleBubbleDataSet;
import java.util.ArrayList;
import java.util.List;

public class CombinedData extends BarLineScatterCandleBubbleData<IBarLineScatterCandleBubbleDataSet<?>> {

    private LineData mLineData;

    private BarData mBarData;

    private ScatterData mScatterData;

    private CandleData mCandleData;

    private BubbleData mBubbleData;

    public CombinedData() {
        super();
    }

    public CombinedData(List<String> xVals) {
        super(xVals);
    }

    public CombinedData(String[] xVals) {
        super(xVals);
    }

    public void setData(LineData data) {
        mLineData = data;
        mDataSets.addAll(data.getDataSets());
        init();
    }

    public void setData(BarData data) {
        mBarData = data;
        mDataSets.addAll(data.getDataSets());
        init();
    }

    public void setData(ScatterData data) {
        mScatterData = data;
        mDataSets.addAll(data.getDataSets());
        init();
    }

    public void setData(CandleData data) {
        mCandleData = data;
        mDataSets.addAll(data.getDataSets());
        init();
    }

    public void setData(BubbleData data) {
        mBubbleData = data;
        mDataSets.addAll(data.getDataSets());
        init();
    }

    public BubbleData getBubbleData() {
        return mBubbleData;
    }

    public LineData getLineData() {
        return mLineData;
    }

    public BarData getBarData() {
        return mBarData;
    }

    public ScatterData getScatterData() {
        return mScatterData;
    }

    public CandleData getCandleData() {
        return mCandleData;
    }

    public List<ChartData> getAllData() {
        List<ChartData> data = new ArrayList<ChartData>();
        if (mLineData != null)
            data.add(mLineData);
        if (mBarData != null)
            data.add(mBarData);
        if (mScatterData != null)
            data.add(mScatterData);
        if (mCandleData != null)
            data.add(mCandleData);
        if (mBubbleData != null)
            data.add(mBubbleData);
        return data;
    }

    @Override
    public void notifyDataChanged() {
        if (mLineData != null)
            mLineData.notifyDataChanged();
        if (mBarData != null)
            mBarData.notifyDataChanged();
        if (mCandleData != null)
            mCandleData.notifyDataChanged();
        if (mScatterData != null)
            mScatterData.notifyDataChanged();
        if (mBubbleData != null)
            mBubbleData.notifyDataChanged();
        init();
    }

    @Override
    public Entry getEntryForHighlight(Highlight highlight) {
        List<ChartData> dataObjects = getAllData();
        if (highlight.getDataIndex() >= dataObjects.size())
            return null;
        ChartData data = dataObjects.get(highlight.getDataIndex());
        if (highlight.getDataSetIndex() >= data.getDataSetCount())
            return null;
        else {
            List<?> entries = data.getDataSetByIndex(highlight.getDataSetIndex()).getEntriesForXIndex(highlight.getXIndex());
            for (Object entry : entries) if (((Entry) entry).getVal() == highlight.getValue() || Float.isNaN(highlight.getValue()))
                return (Entry) entry;
            return null;
        }
    }
}