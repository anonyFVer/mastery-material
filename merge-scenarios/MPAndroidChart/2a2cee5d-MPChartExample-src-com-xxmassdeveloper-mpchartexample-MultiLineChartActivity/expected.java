package com.xxmassdeveloper.mpchartexample;

import android.os.Bundle;
import android.util.Log;
import android.view.Menu;
import android.view.MenuItem;
import android.view.WindowManager;
import android.widget.SeekBar;
import android.widget.SeekBar.OnSeekBarChangeListener;
import android.widget.TextView;
import com.github.mikephil.charting.charts.LineChart;
import com.github.mikephil.charting.components.Legend;
import com.github.mikephil.charting.components.Legend.LegendPosition;
import com.github.mikephil.charting.data.Entry;
import com.github.mikephil.charting.data.LineData;
import com.github.mikephil.charting.data.LineDataSet;
import com.github.mikephil.charting.data.XAxisValue;
import com.github.mikephil.charting.data.filter.Approximator;
import com.github.mikephil.charting.data.filter.Approximator.ApproximatorType;
import com.github.mikephil.charting.highlight.Highlight;
import com.github.mikephil.charting.interfaces.datasets.ILineDataSet;
import com.github.mikephil.charting.listener.OnChartValueSelectedListener;
import com.github.mikephil.charting.utils.ColorTemplate;
import com.xxmassdeveloper.mpchartexample.notimportant.DemoBase;
import java.util.ArrayList;
import java.util.List;

public class MultiLineChartActivity extends DemoBase implements OnSeekBarChangeListener, OnChartValueSelectedListener {

    private LineChart mChart;

    private SeekBar mSeekBarX, mSeekBarY;

    private TextView tvX, tvY;

    @Override
    protected void onCreate(Bundle savedInstanceState) {
        super.onCreate(savedInstanceState);
        getWindow().setFlags(WindowManager.LayoutParams.FLAG_FULLSCREEN, WindowManager.LayoutParams.FLAG_FULLSCREEN);
        setContentView(R.layout.activity_linechart);
        tvX = (TextView) findViewById(R.id.tvXMax);
        tvY = (TextView) findViewById(R.id.tvYMax);
        mSeekBarX = (SeekBar) findViewById(R.id.seekBar1);
        mSeekBarX.setOnSeekBarChangeListener(this);
        mSeekBarY = (SeekBar) findViewById(R.id.seekBar2);
        mSeekBarY.setOnSeekBarChangeListener(this);
        mChart = (LineChart) findViewById(R.id.chart1);
        mChart.setOnChartValueSelectedListener(this);
        mChart.setDrawGridBackground(false);
        mChart.setDescription("");
        mChart.setDrawBorders(false);
        mChart.getAxisLeft().setEnabled(false);
        mChart.getAxisRight().setDrawAxisLine(false);
        mChart.getAxisRight().setDrawGridLines(false);
        mChart.getXAxis().setDrawAxisLine(false);
        mChart.getXAxis().setDrawGridLines(false);
        mChart.setTouchEnabled(true);
        mChart.setDragEnabled(true);
        mChart.setScaleEnabled(true);
        mChart.setPinchZoom(false);
        mSeekBarX.setProgress(20);
        mSeekBarY.setProgress(100);
        Legend l = mChart.getLegend();
        l.setPosition(LegendPosition.RIGHT_OF_CHART);
    }

    @Override
    public boolean onCreateOptionsMenu(Menu menu) {
        getMenuInflater().inflate(R.menu.line, menu);
        return true;
    }

    @Override
    public boolean onOptionsItemSelected(MenuItem item) {
        switch(item.getItemId()) {
            case R.id.actionToggleValues:
                {
                    List<ILineDataSet> sets = mChart.getData().getDataSets();
                    for (ILineDataSet iSet : sets) {
                        LineDataSet set = (LineDataSet) iSet;
                        set.setDrawValues(!set.isDrawValuesEnabled());
                    }
                    mChart.invalidate();
                    break;
                }
            case R.id.actionTogglePinch:
                {
                    if (mChart.isPinchZoomEnabled())
                        mChart.setPinchZoom(false);
                    else
                        mChart.setPinchZoom(true);
                    mChart.invalidate();
                    break;
                }
            case R.id.actionToggleAutoScaleMinMax:
                {
                    mChart.setAutoScaleMinMaxEnabled(!mChart.isAutoScaleMinMaxEnabled());
                    mChart.notifyDataSetChanged();
                    break;
                }
            case R.id.actionToggleHighlight:
                {
                    if (mChart.getData() != null) {
                        mChart.getData().setHighlightEnabled(!mChart.getData().isHighlightEnabled());
                        mChart.invalidate();
                    }
                    break;
                }
            case R.id.actionToggleFilled:
                {
                    List<ILineDataSet> sets = mChart.getData().getDataSets();
                    for (ILineDataSet iSet : sets) {
                        LineDataSet set = (LineDataSet) iSet;
                        if (set.isDrawFilledEnabled())
                            set.setDrawFilled(false);
                        else
                            set.setDrawFilled(true);
                    }
                    mChart.invalidate();
                    break;
                }
            case R.id.actionToggleCircles:
                {
                    List<ILineDataSet> sets = mChart.getData().getDataSets();
                    for (ILineDataSet iSet : sets) {
                        LineDataSet set = (LineDataSet) iSet;
                        if (set.isDrawCirclesEnabled())
                            set.setDrawCircles(false);
                        else
                            set.setDrawCircles(true);
                    }
                    mChart.invalidate();
                    break;
                }
            case R.id.actionSave:
                {
                    mChart.saveToPath("title" + System.currentTimeMillis(), "");
                    break;
                }
            case R.id.animateX:
                {
                    mChart.animateX(3000);
                    break;
                }
            case R.id.animateY:
                {
                    mChart.animateY(3000);
                    break;
                }
            case R.id.animateXY:
                {
                    mChart.animateXY(3000, 3000);
                    break;
                }
        }
        return true;
    }

    private int[] mColors = new int[] { ColorTemplate.VORDIPLOM_COLORS[0], ColorTemplate.VORDIPLOM_COLORS[1], ColorTemplate.VORDIPLOM_COLORS[2] };

    @Override
    public void onProgressChanged(SeekBar seekBar, int progress, boolean fromUser) {
        mChart.resetTracking();
        tvX.setText("" + (mSeekBarX.getProgress()));
        tvY.setText("" + (mSeekBarY.getProgress()));
        ArrayList<XAxisValue> xVals = new ArrayList<XAxisValue>();
        for (int i = 0; i < mSeekBarX.getProgress(); i++) {
            xVals.add(new XAxisValue(i, i + ""));
        }
        ArrayList<ILineDataSet> dataSets = new ArrayList<ILineDataSet>();
        for (int z = 0; z < 3; z++) {
            ArrayList<Entry> values = new ArrayList<Entry>();
            for (int i = 0; i < mSeekBarX.getProgress(); i++) {
                double val = (Math.random() * mSeekBarY.getProgress()) + 3;
                values.add(new Entry((float) val, i));
            }
            LineDataSet d = new LineDataSet(values, "DataSet " + (z + 1));
            d.setLineWidth(2.5f);
            d.setCircleRadius(4f);
            int color = mColors[z % mColors.length];
            d.setColor(color);
            d.setCircleColor(color);
            dataSets.add(d);
        }
        ((LineDataSet) dataSets.get(0)).enableDashedLine(10, 10, 0);
        ((LineDataSet) dataSets.get(0)).setColors(ColorTemplate.VORDIPLOM_COLORS);
        ((LineDataSet) dataSets.get(0)).setCircleColors(ColorTemplate.VORDIPLOM_COLORS);
        LineData data = new LineData(xVals, dataSets);
        mChart.setData(data);
        mChart.invalidate();
    }

    @Override
    public void onValueSelected(Entry e, int dataSetIndex, Highlight h) {
        Log.i("VAL SELECTED", "Value: " + e.getVal() + ", xIndex: " + e.getXIndex() + ", DataSet index: " + dataSetIndex);
    }

    @Override
    public void onNothingSelected() {
    }

    @Override
    public void onStartTrackingTouch(SeekBar seekBar) {
    }

    @Override
    public void onStopTrackingTouch(SeekBar seekBar) {
    }
}