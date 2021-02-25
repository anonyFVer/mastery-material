package com.xxmassdeveloper.mpchartexample;

import android.os.Bundle;
import android.util.Log;
import android.view.Menu;
import android.view.MenuItem;
import android.view.WindowManager;
import android.widget.SeekBar;
import android.widget.SeekBar.OnSeekBarChangeListener;
import android.widget.TextView;
import android.widget.Toast;
import com.github.mikephil.charting.charts.BarChart;
import com.github.mikephil.charting.components.Legend;
import com.github.mikephil.charting.components.Legend.LegendPosition;
import com.github.mikephil.charting.components.XAxis;
import com.github.mikephil.charting.components.XAxis.XAxisPosition;
import com.github.mikephil.charting.components.YAxis;
import com.github.mikephil.charting.data.BarData;
import com.github.mikephil.charting.data.BarDataSet;
import com.github.mikephil.charting.data.BarEntry;
import com.github.mikephil.charting.data.Entry;
import com.github.mikephil.charting.data.LineDataSet;
import com.github.mikephil.charting.data.filter.Approximator;
import com.github.mikephil.charting.data.filter.Approximator.ApproximatorType;
import com.github.mikephil.charting.highlight.Highlight;
import com.github.mikephil.charting.interfaces.datasets.IBarDataSet;
import com.github.mikephil.charting.listener.OnChartValueSelectedListener;
import com.github.mikephil.charting.utils.ColorTemplate;
import com.xxmassdeveloper.mpchartexample.custom.MyValueFormatter;
import com.xxmassdeveloper.mpchartexample.custom.MyYAxisValueFormatter;
import com.xxmassdeveloper.mpchartexample.notimportant.DemoBase;
import java.util.ArrayList;
import java.util.List;

public class StackedBarActivity extends DemoBase implements OnSeekBarChangeListener, OnChartValueSelectedListener {

    private BarChart mChart;

    private SeekBar mSeekBarX, mSeekBarY;

    private TextView tvX, tvY;

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
        mChart.setMaxVisibleValueCount(60);
        mChart.setPinchZoom(false);
        mChart.setDrawGridBackground(false);
        mChart.setDrawBarShadow(false);
        mChart.setDrawValueAboveBar(false);
        YAxis leftAxis = mChart.getAxisLeft();
        leftAxis.setValueFormatter(new MyYAxisValueFormatter());
        leftAxis.setAxisMinValue(0f);
        mChart.getAxisRight().setEnabled(false);
        XAxis xLabels = mChart.getXAxis();
        xLabels.setPosition(XAxisPosition.TOP);
        mSeekBarX.setProgress(12);
        mSeekBarY.setProgress(100);
        Legend l = mChart.getLegend();
        l.setPosition(LegendPosition.BELOW_CHART_RIGHT);
        l.setFormSize(8f);
        l.setFormToTextSpace(4f);
        l.setXEntrySpace(6f);
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
                    List<IBarDataSet> sets = mChart.getData().getDataSets();
                    for (IBarDataSet iSet : sets) {
                        BarDataSet set = (BarDataSet) iSet;
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
            case R.id.actionToggleBarBorders:
                {
                    for (IBarDataSet set : mChart.getData().getDataSets()) ((BarDataSet) set).setBarBorderWidth(set.getBarBorderWidth() == 1.f ? 0.f : 1.f);
                    mChart.invalidate();
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
                    if (mChart.saveToGallery("title" + System.currentTimeMillis(), 50)) {
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
        ArrayList<String> xVals = new ArrayList<String>();
        for (int i = 0; i < mSeekBarX.getProgress() + 1; i++) {
            xVals.add(mMonths[i % mMonths.length]);
        }
        ArrayList<BarEntry> yVals1 = new ArrayList<BarEntry>();
        for (int i = 0; i < mSeekBarX.getProgress() + 1; i++) {
            float mult = (mSeekBarY.getProgress() + 1);
            float val1 = (float) (Math.random() * mult) + mult / 3;
            float val2 = (float) (Math.random() * mult) + mult / 3;
            float val3 = (float) (Math.random() * mult) + mult / 3;
            yVals1.add(new BarEntry(new float[] { val1, val2, val3 }, i));
        }
        BarDataSet set1;
        if (mChart.getData() != null && mChart.getData().getDataSetCount() > 0) {
            set1 = (BarDataSet) mChart.getData().getDataSetByIndex(0);
            set1.setYVals(yVals1);
            mChart.getData().setXVals(xVals);
            mChart.getData().notifyDataChanged();
            mChart.notifyDataSetChanged();
        } else {
            set1 = new BarDataSet(yVals1, "Statistics Vienna 2014");
            set1.setColors(getColors());
            set1.setStackLabels(new String[] { "Births", "Divorces", "Marriages" });
            ArrayList<IBarDataSet> dataSets = new ArrayList<IBarDataSet>();
            dataSets.add(set1);
            BarData data = new BarData(xVals, dataSets);
            data.setValueFormatter(new MyValueFormatter());
            mChart.setData(data);
        }
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
        BarEntry entry = (BarEntry) e;
        if (entry.getVals() != null)
            Log.i("VAL SELECTED", "Value: " + entry.getVals()[h.getStackIndex()]);
        else
            Log.i("VAL SELECTED", "Value: " + entry.getVal());
    }

    @Override
    public void onNothingSelected() {
    }

    private int[] getColors() {
        int stacksize = 3;
        int[] colors = new int[stacksize];
        for (int i = 0; i < stacksize; i++) {
            colors[i] = ColorTemplate.VORDIPLOM_COLORS[i];
        }
        return colors;
    }
}