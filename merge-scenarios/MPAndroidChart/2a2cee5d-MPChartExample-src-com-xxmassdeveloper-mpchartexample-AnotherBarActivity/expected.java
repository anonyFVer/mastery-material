package com.xxmassdeveloper.mpchartexample;

import android.os.Bundle;
import android.view.Menu;
import android.view.MenuItem;
import android.view.WindowManager;
import android.widget.SeekBar;
import android.widget.SeekBar.OnSeekBarChangeListener;
import android.widget.TextView;
import android.widget.Toast;
import com.github.mikephil.charting.charts.BarChart;
import com.github.mikephil.charting.components.XAxis;
import com.github.mikephil.charting.components.XAxis.XAxisPosition;
import com.github.mikephil.charting.data.BarData;
import com.github.mikephil.charting.data.BarDataSet;
import com.github.mikephil.charting.data.BarEntry;
import com.github.mikephil.charting.data.XAxisValue;
import com.github.mikephil.charting.data.filter.Approximator;
import com.github.mikephil.charting.data.filter.Approximator.ApproximatorType;
import com.github.mikephil.charting.interfaces.datasets.IBarDataSet;
import com.github.mikephil.charting.interfaces.datasets.IDataSet;
import com.github.mikephil.charting.utils.ColorTemplate;
import com.xxmassdeveloper.mpchartexample.notimportant.DemoBase;
import java.util.ArrayList;

public class AnotherBarActivity extends DemoBase implements OnSeekBarChangeListener {

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
        mChart.setDescription("");
        mChart.setMaxVisibleValueCount(60);
        mChart.setPinchZoom(false);
        mChart.setDrawBarShadow(false);
        mChart.setDrawGridBackground(false);
        XAxis xAxis = mChart.getXAxis();
        xAxis.setPosition(XAxisPosition.BOTTOM);
        xAxis.setSpaceBetweenLabels(0);
        xAxis.setDrawGridLines(false);
        mChart.getAxisLeft().setDrawGridLines(false);
        mSeekBarX.setProgress(10);
        mSeekBarY.setProgress(100);
        mChart.animateY(2500);
        mChart.getLegend().setEnabled(false);
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
                    for (IDataSet set : mChart.getData().getDataSets()) set.setDrawValues(!set.isDrawValuesEnabled());
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
        ArrayList<BarEntry> yVals1 = new ArrayList<BarEntry>();
        for (int i = 0; i < mSeekBarX.getProgress() + 1; i++) {
            float mult = (mSeekBarY.getProgress() + 1);
            float val1 = (float) (Math.random() * mult) + mult / 3;
            yVals1.add(new BarEntry((int) val1, i));
        }
        ArrayList<XAxisValue> xVals = new ArrayList<XAxisValue>();
        for (int i = 0; i < mSeekBarX.getProgress() + 1; i++) {
            XAxisValue xValue = new XAxisValue(i, (int) yVals1.get(i).getVal() + "");
            xVals.add(xValue);
        }
        BarDataSet set1;
        if (mChart.getData() != null && mChart.getData().getDataSetCount() > 0) {
            set1 = (BarDataSet) mChart.getData().getDataSetByIndex(0);
            set1.setYVals(yVals1);
            mChart.getData().setXVals(xVals);
            mChart.getData().notifyDataChanged();
            mChart.notifyDataSetChanged();
        } else {
            set1 = new BarDataSet(yVals1, "Data Set");
            set1.setColors(ColorTemplate.VORDIPLOM_COLORS);
            set1.setDrawValues(false);
            ArrayList<IBarDataSet> dataSets = new ArrayList<IBarDataSet>();
            dataSets.add(set1);
            BarData data = new BarData(xVals, dataSets);
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
}