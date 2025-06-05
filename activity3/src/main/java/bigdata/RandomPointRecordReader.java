package bigdata;

import org.apache.hadoop.mapreduce.RecordReader;
import org.apache.hadoop.mapreduce.TaskAttemptContext;
import org.apache.hadoop.mapreduce.InputSplit;
import org.apache.hadoop.io.NullWritable;

import java.io.IOException;
import java.util.Random;

public class RandomPointRecordReader extends RecordReader<NullWritable, Point2DWritable> {
    private int numPoints;
    private int pointsGenerated = 0;
    private Random random = new Random();
    private Point2DWritable currentValue = new Point2DWritable();

    @Override
    public void initialize(InputSplit split, TaskAttemptContext context) throws IOException {
        FakeInputSplit fakeSplit = (FakeInputSplit) split;
        this.numPoints = fakeSplit.getNumPoints();
    }

    @Override
    public boolean nextKeyValue() throws IOException {
        if (pointsGenerated < numPoints) {
            double x = random.nextDouble();
            double y = random.nextDouble();
            currentValue.setPoint(new java.awt.geom.Point2D.Double(x, y));
            pointsGenerated++;
            return true;
        }
        return false;
    }

    @Override
    public NullWritable getCurrentKey() {
        return NullWritable.get();
    }

    @Override
    public Point2DWritable getCurrentValue() {
        return currentValue;
    }

    @Override
    public float getProgress() {
        return pointsGenerated / (float) numPoints;
    }

    @Override
    public void close() {}
}
