package com.xxmassdeveloper.mpchartexample;

import android.graphics.Color;
import android.graphics.Typeface;
import android.os.Bundle;
import android.util.Log;
import android.view.Menu;
import android.view.MenuItem;
import android.view.WindowManager;
import android.widget.SeekBar;
import android.widget.SeekBar.OnSeekBarChangeListener;
import android.widget.TextView;
import com.github.mikephil.charting.charts.BarChart;
import com.github.mikephil.charting.components.Legend;
import com.github.mikephil.charting.components.Legend.LegendPosition;
import com.github.mikephil.charting.components.XAxis;
import com.github.mikephil.charting.components.YAxis;
import com.github.mikephil.charting.data.BarData;
import com.github.mikephil.charting.data.BarDataSet;
import com.github.mikephil.charting.data.BarEntry;
import com.github.mikephil.charting.data.Entry;
import com.github.mikephil.charting.data.XAxisValue;
import com.github.mikephil.charting.formatter.LargeValueFormatter;
import com.github.mikephil.charting.highlight.Highlight;
import com.github.mikephil.charting.interfaces.datasets.IBarDataSet;
import com.github.mikephil.charting.listener.OnChartValueSelectedListener;
import com.xxmassdeveloper.mpchartexample.custom.MyMarkerView;
import com.xxmassdeveloper.mpchartexample.notimportant.DemoBase;
import java.util.ArrayList;

public class BarChartActivityMultiDataset extends DemoBase implements OnSeekBarChangeListener, OnChartValueSelectedListener {

    private BarChart mChart;

    private SeekBar mSeekBarX, mSeekBarY;

    private TextView tvX, tvY;

    private Typeface tf;

    @Override
    protected void onCreate(Bundle savedInstanceState) {
        super.onCreate(savedInstanceState);
        getWindow().setFlags(WindowManager.LayoutParams.FLAG_FULLSCREEN, WindowManager.LayoutParams.FLAG_FULLSCREEN);
        setContentView(R.layout.activity_barchart);
        tvX = (TextView) findViewById(R.id.tvXMax);
        tvY = (TextView) findViewById(R.id.tvYMax);
        mSeekBarX = (SeekBar) findViewById(R.id.seekBar1);
        mSeekBarX.setOnSeekBarChangeListener(this);
        mSeekBarY = (SeekBar) findViewById(R.id.seekBar2);
        mSeekBarY.setOnSeekBarChangeListener(this);
        mChart = (BarChart) findViewById(R.id.chart1);
        mChart.setOnChartValueSelectedListener(this);
        mChart.setDescription("");
        mChart.setPinchZoom(false);
        mChart.setDrawBarShadow(false);
        mChart.setDrawGridBackground(false);
        MyMarkerView mv = new MyMarkerView(this, R.layout.custom_marker_view);
        mChart.setMarkerView(mv);
        mSeekBarX.setProgress(10);
        mSeekBarY.setProgress(100);
        tf = Typeface.createFromAsset(getAssets(), "OpenSans-Regular.ttf");
        Legend l = mChart.getLegend();
        l.setPosition(LegendPosition.RIGHT_OF_CHART_INSIDE);
        l.setTypeface(tf);
        l.setYOffset(0f);
        l.setYEntrySpace(0f);
        l.setTextSize(8f);
        XAxis xl = mChart.getXAxis();
        xl.setTypeface(tf);
        YAxis leftAxis = mChart.getAxisLeft();
        leftAxis.setTypeface(tf);
        leftAxis.setValueFormatter(new LargeValueFormatter());
        leftAxis.setDrawGridLines(false);
        leftAxis.setSpaceTop(30f);
        leftAxis.setAxisMinValue(0f);
        mChart.getAxisRight().setEnabled(false);
    }

    @Override
    public boolean onCreateOptionsMenu(Menu menu) {
        getMenuInflater().inflate(R.menu.bar, menu);
        return true;
    }

    @Override
    public boolean onOptionsItemSelected(MenuItem item) {
        switch(item.getItemId()) {
            case R.id.actionToggleValues:
                {
                    for (IBarDataSet set : mChart.getData().getDataSets()) set.setDrawValues(!set.isDrawValuesEnabled());
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
            case R.id.actionToggleHighlightArrow:
                {
                    if (mChart.isDrawHighlightArrowEnabled())
                        mChart.setDrawHighlightArrow(false);
                    else
                        mChart.setDrawHighlightArrow(true);
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

    @Override
    public void onProgressChanged(SeekBar seekBar, int progress, boolean fromUser) {
        tvX.setText("" + (mSeekBarX.getProgress() * 3));
        tvY.setText("" + (mSeekBarY.getProgress()));
        ArrayList<XAxisValue> xVals = new ArrayList<XAxisValue>();
        for (int i = 0; i < mSeekBarX.getProgress(); i++) {
            XAxisValue value = new XAxisValue(i, (i + 1990) + "");
            xVals.add(value);
        }
        ArrayList<BarEntry> yVals1 = new ArrayList<BarEntry>();
        ArrayList<BarEntry> yVals2 = new ArrayList<BarEntry>();
        ArrayList<BarEntry> yVals3 = new ArrayList<BarEntry>();
        float mult = mSeekBarY.getProgress() * 1000f;
        for (int i = 0; i < mSeekBarX.getProgress(); i++) {
            float val = (float) (Math.random() * mult) + 3;
            yVals1.add(new BarEntry(val, i));
        }
        for (int i = 0; i < mSeekBarX.getProgress(); i++) {
            float val = (float) (Math.random() * mult) + 3;
            yVals2.add(new BarEntry(val, i));
        }
        for (int i = 0; i < mSeekBarX.getProgress(); i++) {
            float val = (float) (Math.random() * mult) + 3;
            yVals3.add(new BarEntry(val, i));
        }
        BarDataSet set1 = new BarDataSet(yVals1, "Company A");
        set1.setColor(Color.rgb(104, 241, 175));
        BarDataSet set2 = new BarDataSet(yVals2, "Company B");
        set2.setColor(Color.rgb(164, 228, 251));
        BarDataSet set3 = new BarDataSet(yVals3, "Company C");
        set3.setColor(Color.rgb(242, 247, 158));
        ArrayList<IBarDataSet> dataSets = new ArrayList<IBarDataSet>();
        dataSets.add(set1);
        dataSets.add(set2);
        dataSets.add(set3);
        BarData data = new BarData(xVals, dataSets);
        data.setGroupSpace(80f);
        data.setValueTypeface(tf);
        mChart.setData(data);
        mChart.invalidate();
    }

    @Override
    public void onStartTrackingTouch(SeekBar seekBar) {
    }

    @Override
    public void onStopTrackingTouch(SeekBar seekBar) {
    }

    @Override
    public void onValueSelected(Entry e, int dataSetIndex, Highlight h) {
        Log.i("Activity", "Selected: " + e.toString() + ", dataSet: " + dataSetIndex);
    }

    @Override
    public void onNothingSelected() {
        Log.i("Activity", "Nothing selected.");
    }
}