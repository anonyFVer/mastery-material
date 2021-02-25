package com.example.mpchartexample;

import android.app.Activity;
import android.os.Bundle;
import android.util.Log;
import android.view.Menu;
import android.view.MenuItem;
import android.view.WindowManager;
import android.widget.SeekBar;
import android.widget.SeekBar.OnSeekBarChangeListener;
import android.widget.TextView;
import com.github.mikephil.charting.charts.LineChart;
import com.github.mikephil.charting.data.ChartData;
import com.github.mikephil.charting.data.DataSet;
import com.github.mikephil.charting.data.Entry;
import com.github.mikephil.charting.data.filter.Approximator;
import com.github.mikephil.charting.data.filter.Approximator.ApproximatorType;
import com.github.mikephil.charting.interfaces.OnChartValueSelectedListener;
import com.github.mikephil.charting.utils.ColorTemplate;
import com.github.mikephil.charting.utils.Highlight;
import java.util.ArrayList;

public class LineChartActivity extends Activity implements OnSeekBarChangeListener, OnChartValueSelectedListener {

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
        ColorTemplate ct = new ColorTemplate();
        ct.addColorsForDataSets(new int[] { R.color.colorful_1 }, this);
        mChart = (LineChart) findViewById(R.id.chart1);
        mChart.setOnChartValueSelectedListener(this);
        mChart.setColorTemplate(ct);
        mChart.setStartAtZero(false);
        mChart.setDrawYValues(false);
        mChart.setLineWidth(4f);
        mChart.setCircleSize(4f);
        mChart.setYLegendCount(6);
        mChart.setHighlightEnabled(true);
        mChart.setTouchEnabled(true);
        mChart.setDragEnabled(true);
        mChart.setPinchZoom(true);
        MyMarkerView mv = new MyMarkerView(this, R.layout.custom_marker_view);
        mv.setOffsets(-mv.getMeasuredWidth() / 2, -mv.getMeasuredHeight());
        mChart.setMarkerView(mv);
        mChart.setHighlightIndicatorEnabled(false);
        mChart.enableDashedLine(10f, 5f, 0f);
        mSeekBarX.setProgress(45);
        mSeekBarY.setProgress(100);
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
                    if (mChart.isDrawYValuesEnabled())
                        mChart.setDrawYValues(false);
                    else
                        mChart.setDrawYValues(true);
                    mChart.invalidate();
                    break;
                }
            case R.id.actionToggleHighlight:
                {
                    if (mChart.isHighlightEnabled())
                        mChart.setHighlightEnabled(false);
                    else
                        mChart.setHighlightEnabled(true);
                    mChart.invalidate();
                    break;
                }
            case R.id.actionToggleFilled:
                {
                    if (mChart.isDrawFilledEnabled())
                        mChart.setDrawFilled(false);
                    else
                        mChart.setDrawFilled(true);
                    mChart.invalidate();
                    break;
                }
            case R.id.actionToggleCircles:
                {
                    if (mChart.isDrawCirclesEnabled())
                        mChart.setDrawCircles(false);
                    else
                        mChart.setDrawCircles(true);
                    mChart.invalidate();
                    break;
                }
            case R.id.actionToggleStartzero:
                {
                    if (mChart.isStartAtZeroEnabled())
                        mChart.setStartAtZero(false);
                    else
                        mChart.setStartAtZero(true);
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
            case R.id.actionToggleAdjustXLegend:
                {
                    if (mChart.isAdjustXLegendEnabled())
                        mChart.setAdjustXLegend(false);
                    else
                        mChart.setAdjustXLegend(true);
                    mChart.invalidate();
                    break;
                }
            case R.id.actionToggleFilter:
                {
                    Approximator a = new Approximator(ApproximatorType.DOUGLAS_PEUCKER, 35);
                    if (!mChart.isFilteringEnabled()) {
                        mChart.enableFiltering(a);
                    } else {
                        mChart.disableFiltering();
                    }
                    mChart.invalidate();
                    break;
                }
            case R.id.actionDashedLine:
                {
                    if (!mChart.isDashedLineEnabled()) {
                        mChart.enableDashedLine(10f, 5f, 0f);
                    } else {
                        mChart.disableDashedLine();
                    }
                    mChart.invalidate();
                    break;
                }
            case R.id.actionSave:
                {
                    mChart.saveToPath("title" + System.currentTimeMillis(), "");
                    break;
                }
        }
        return true;
    }

    @Override
    public void onProgressChanged(SeekBar seekBar, int progress, boolean fromUser) {
        tvX.setText("" + (mSeekBarX.getProgress() + 1));
        tvY.setText("" + (mSeekBarY.getProgress()));
        ArrayList<String> xVals = new ArrayList<String>();
        for (int i = 0; i < mSeekBarX.getProgress(); i++) {
            xVals.add((i) + "");
        }
        ArrayList<Entry> yVals = new ArrayList<Entry>();
        for (int i = 0; i < mSeekBarX.getProgress(); i++) {
            float mult = (mSeekBarY.getProgress() + 1);
            float val = (float) (Math.random() * mult * 1) + 3;
            yVals.add(new Entry(val, i));
        }
        DataSet set1 = new DataSet(yVals, 0);
        ArrayList<DataSet> dataSets = new ArrayList<DataSet>();
        dataSets.add(set1);
        ChartData data = new ChartData(xVals, dataSets);
        mChart.setData(data);
        mChart.invalidate();
    }

    @Override
    public void onValuesSelected(Entry[] values, Highlight[] highlights) {
        Log.i("VALS SELECTED", "Value: " + values[0].getVal() + ", xIndex: " + highlights[0].getXIndex() + ", DataSet index: " + highlights[0].getDataSetIndex());
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