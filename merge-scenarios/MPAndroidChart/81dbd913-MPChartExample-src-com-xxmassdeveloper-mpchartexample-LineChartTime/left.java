package com.xxmassdeveloper.mpchartexample;

import android.graphics.Color;
import android.graphics.Typeface;
import android.os.Bundle;
import android.view.Menu;
import android.view.MenuItem;
import android.view.View;
import android.view.WindowManager;
import android.widget.SeekBar;
import android.widget.SeekBar.OnSeekBarChangeListener;
import android.widget.TextView;
import android.widget.Toast;
import com.github.mikephil.charting.charts.LineChart;
import com.github.mikephil.charting.components.AxisBase;
import com.github.mikephil.charting.components.Legend;
import com.github.mikephil.charting.components.Legend.LegendForm;
import com.github.mikephil.charting.components.Legend.LegendPosition;
import com.github.mikephil.charting.components.XAxis;
import com.github.mikephil.charting.components.YAxis;
import com.github.mikephil.charting.components.YAxis.AxisDependency;
import com.github.mikephil.charting.data.Entry;
import com.github.mikephil.charting.data.LineData;
import com.github.mikephil.charting.data.LineDataSet;
import com.github.mikephil.charting.formatter.AxisValueFormatter;
import com.github.mikephil.charting.interfaces.datasets.ILineDataSet;
import com.github.mikephil.charting.utils.ColorTemplate;
import com.xxmassdeveloper.mpchartexample.notimportant.DemoBase;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Date;
import java.util.List;

public class LineChartTime extends DemoBase implements OnSeekBarChangeListener {

    private LineChart mChart;

    private SeekBar mSeekBarX;

    private TextView tvX;

    @Override
    protected void onCreate(Bundle savedInstanceState) {
        super.onCreate(savedInstanceState);
        getWindow().setFlags(WindowManager.LayoutParams.FLAG_FULLSCREEN, WindowManager.LayoutParams.FLAG_FULLSCREEN);
        setContentView(R.layout.activity_linechart_time);
        tvX = (TextView) findViewById(R.id.tvXMax);
        mSeekBarX = (SeekBar) findViewById(R.id.seekBar1);
        mSeekBarX.setProgress(100);
        tvX.setText("100");
        mSeekBarX.setOnSeekBarChangeListener(this);
        mChart = (LineChart) findViewById(R.id.chart1);
        mChart.setHardwareAccelerationEnabled(true);
        mChart.setDescription("");
        mChart.setNoDataTextDescription("You need to provide data for the chart.");
        mChart.setTouchEnabled(true);
        mChart.setDragDecelerationFrictionCoef(0.9f);
        mChart.setDragEnabled(true);
        mChart.setScaleEnabled(true);
        mChart.setDrawGridBackground(false);
        mChart.setHighlightPerDragEnabled(true);
        mChart.setBackgroundColor(Color.WHITE);
        mChart.setViewPortOffsets(0f, 0f, 0f, 0f);
        setData(100, 30);
        mChart.invalidate();
        Typeface tf = Typeface.createFromAsset(getAssets(), "OpenSans-Light.ttf");
        Legend l = mChart.getLegend();
        l.setEnabled(false);
        XAxis xAxis = mChart.getXAxis();
        xAxis.setPosition(XAxis.XAxisPosition.TOP_INSIDE);
        xAxis.setTypeface(tf);
        xAxis.setTextSize(10f);
        xAxis.setTextColor(Color.WHITE);
        xAxis.setDrawAxisLine(false);
        xAxis.setDrawGridLines(true);
        xAxis.setTextColor(Color.rgb(255, 192, 56));
        xAxis.setCenterAxisLabels(true);
        xAxis.setGranularity(60000L);
        xAxis.setValueFormatter(new AxisValueFormatter() {

            private SimpleDateFormat mFormat = new SimpleDateFormat("dd MMM HH:mm");

            @Override
            public String getFormattedValue(float value, AxisBase axis) {
                return mFormat.format(new Date((long) value));
            }

            @Override
            public int getDecimalDigits() {
                return 0;
            }
        });
        YAxis leftAxis = mChart.getAxisLeft();
        leftAxis.setPosition(YAxis.YAxisLabelPosition.INSIDE_CHART);
        leftAxis.setTypeface(tf);
        leftAxis.setTextColor(ColorTemplate.getHoloBlue());
        leftAxis.setDrawGridLines(true);
        leftAxis.setGranularityEnabled(true);
        leftAxis.setAxisMinValue(0f);
        leftAxis.setAxisMaxValue(170f);
        leftAxis.setYOffset(-9f);
        leftAxis.setTextColor(Color.rgb(255, 192, 56));
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
        tvX.setText("" + (mSeekBarX.getProgress()));
        setData(mSeekBarX.getProgress(), 50);
        mChart.invalidate();
    }

    private void setData(int count, float range) {
        long now = System.currentTimeMillis();
        long hourMillis = 3600000L;
        ArrayList<Entry> values = new ArrayList<Entry>();
        float from = now - (count / 2) * hourMillis;
        float to = now + (count / 2) * hourMillis;
        for (float x = from; x < to; x += hourMillis) {
            float y = getRandom(range, 50);
            values.add(new Entry(x, y));
        }
        LineDataSet set1 = new LineDataSet(values, "DataSet 1");
        set1.setAxisDependency(AxisDependency.LEFT);
        set1.setColor(ColorTemplate.getHoloBlue());
        set1.setValueTextColor(ColorTemplate.getHoloBlue());
        set1.setLineWidth(1.5f);
        set1.setDrawCircles(false);
        set1.setDrawValues(false);
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