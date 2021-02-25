package com.xxmassdeveloper.mpchartexample;

import android.graphics.Color;
import android.graphics.Typeface;
import android.os.Bundle;
import android.view.Menu;
import android.view.MenuItem;
import android.view.WindowManager;
import android.widget.SeekBar;
import android.widget.SeekBar.OnSeekBarChangeListener;
import android.widget.TextView;
import android.widget.Toast;
import com.github.mikephil.charting.charts.LineChart;
import com.github.mikephil.charting.components.Legend;
import com.github.mikephil.charting.components.Legend.LegendForm;
import com.github.mikephil.charting.components.Legend.LegendPosition;
import com.github.mikephil.charting.components.XAxis;
import com.github.mikephil.charting.components.YAxis;
import com.github.mikephil.charting.components.YAxis.AxisDependency;
import com.github.mikephil.charting.data.Entry;
import com.github.mikephil.charting.data.LineData;
import com.github.mikephil.charting.data.LineDataSet;
import com.github.mikephil.charting.interfaces.datasets.ILineDataSet;
import com.github.mikephil.charting.utils.ColorTemplate;
import com.xxmassdeveloper.mpchartexample.notimportant.DemoBase;
import java.util.ArrayList;
import java.util.List;

public class LineChartTime extends DemoBase implements OnSeekBarChangeListener {

    private LineChart mChart;

    private SeekBar mSeekBarX, mSeekBarY;

    private TextView tvX, tvY;

    private long curTime = System.currentTimeMillis();

    @Override
    protected void onCreate(Bundle savedInstanceState) {
        super.onCreate(savedInstanceState);
        getWindow().setFlags(WindowManager.LayoutParams.FLAG_FULLSCREEN, WindowManager.LayoutParams.FLAG_FULLSCREEN);
        setContentView(R.layout.activity_linechart);
        tvX = (TextView) findViewById(R.id.tvXMax);
        tvY = (TextView) findViewById(R.id.tvYMax);
        mSeekBarX = (SeekBar) findViewById(R.id.seekBar1);
        mSeekBarY = (SeekBar) findViewById(R.id.seekBar2);
        mSeekBarX.setProgress(45);
        mSeekBarY.setProgress(100);
        mSeekBarY.setOnSeekBarChangeListener(this);
        mSeekBarX.setOnSeekBarChangeListener(this);
        mChart = (LineChart) findViewById(R.id.chart1);
        mChart.setLogEnabled(true);
        mChart.setDescription("");
        mChart.setNoDataTextDescription("You need to provide data for the chart.");
        mChart.setTouchEnabled(true);
        mChart.setDragDecelerationFrictionCoef(0.9f);
        mChart.setDragEnabled(true);
        mChart.setScaleEnabled(true);
        mChart.setDrawGridBackground(false);
        mChart.setHighlightPerDragEnabled(true);
        mChart.setPinchZoom(true);
        mChart.setBackgroundColor(Color.LTGRAY);
        setData(20, 30);
        mChart.invalidate();
        Typeface tf = Typeface.createFromAsset(getAssets(), "OpenSans-Regular.ttf");
        Legend l = mChart.getLegend();
        l.setForm(LegendForm.LINE);
        l.setTypeface(tf);
        l.setTextSize(11f);
        l.setTextColor(Color.WHITE);
        l.setPosition(LegendPosition.BELOW_CHART_LEFT);
        XAxis xAxis = mChart.getXAxis();
        xAxis.setTypeface(tf);
        xAxis.setTextSize(12f);
        xAxis.setTextColor(Color.WHITE);
        xAxis.setDrawAxisLine(false);
        xAxis.setAxisMinValue(5000);
        xAxis.setAxisMaxValue(30000);
        YAxis leftAxis = mChart.getAxisLeft();
        leftAxis.setTypeface(tf);
        leftAxis.setTextColor(ColorTemplate.getHoloBlue());
        leftAxis.setAxisMaxValue(200f);
        leftAxis.setAxisMinValue(0f);
        leftAxis.setDrawGridLines(true);
        leftAxis.setGranularityEnabled(true);
        YAxis rightAxis = mChart.getAxisRight();
        rightAxis.setEnabled(false);
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
            case R.id.actionToggleCubic:
                {
                    List<ILineDataSet> sets = mChart.getData().getDataSets();
                    for (ILineDataSet iSet : sets) {
                        LineDataSet set = (LineDataSet) iSet;
                        if (set.getMode() == LineDataSet.Mode.CUBIC_BEZIER)
                            set.setMode(LineDataSet.Mode.LINEAR);
                        else
                            set.setMode(LineDataSet.Mode.CUBIC_BEZIER);
                    }
                    mChart.invalidate();
                    break;
                }
            case R.id.actionToggleStepped:
                {
                    List<ILineDataSet> sets = mChart.getData().getDataSets();
                    for (ILineDataSet iSet : sets) {
                        LineDataSet set = (LineDataSet) iSet;
                        if (set.getMode() == LineDataSet.Mode.STEPPED)
                            set.setMode(LineDataSet.Mode.LINEAR);
                        else
                            set.setMode(LineDataSet.Mode.STEPPED);
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
            case R.id.actionSave:
                {
                    if (mChart.saveToPath("title" + System.currentTimeMillis(), "")) {
                        Toast.makeText(getApplicationContext(), "Saving SUCCESSFUL!", Toast.LENGTH_SHORT).show();
                    } else
                        Toast.makeText(getApplicationContext(), "Saving FAILED!", Toast.LENGTH_SHORT).show();
                    break;
                }
        }
        return true;
    }

    @Override
    public void onProgressChanged(SeekBar seekBar, int progress, boolean fromUser) {
        tvX.setText("" + (mSeekBarX.getProgress() + 1));
        tvY.setText("" + (mSeekBarY.getProgress()));
        setData(mSeekBarX.getProgress() + 1, mSeekBarY.getProgress());
        mChart.invalidate();
        System.out.println("xmin: " + mChart.getXAxis().getAxisMinimum());
        System.out.println("xmax: " + mChart.getXAxis().getAxisMaximum());
        System.out.println("lvx: " + mChart.getLowestVisibleX());
        System.out.println("hvx: " + mChart.getHighestVisibleX());
    }

    private void setData(int count, float range) {
        ArrayList<Entry> yVals1 = new ArrayList<Entry>();
        for (int i = 0; i < count; i++) {
            float mult = range / 2f;
            float val = (float) (Math.random() * mult) + 50;
        }
        yVals1.add(new Entry(100, 10000));
        yVals1.add(new Entry(130, 15000));
        yVals1.add(new Entry(120, 20000));
        LineDataSet set1 = new LineDataSet(yVals1, "DataSet 1");
        set1.setAxisDependency(AxisDependency.LEFT);
        set1.setColor(ColorTemplate.getHoloBlue());
        set1.setCircleColor(Color.WHITE);
        set1.setLineWidth(2f);
        set1.setCircleRadius(3f);
        set1.setFillAlpha(65);
        set1.setFillColor(ColorTemplate.getHoloBlue());
        set1.setHighLightColor(Color.rgb(244, 117, 117));
        set1.setDrawCircleHole(false);
        LineData data = new LineData(set1);
        data.setValueTextColor(Color.WHITE);
        data.setValueTextSize(9f);
        mChart.setData(data);
    }

    @Override
    public void onStartTrackingTouch(SeekBar seekBar) {
    }

    @Override
    public void onStopTrackingTouch(SeekBar seekBar) {
    }
}