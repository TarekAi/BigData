package bigdata;

import org.apache.hadoop.mapreduce.InputFormat;
import org.apache.hadoop.mapreduce.InputSplit;
import org.apache.hadoop.mapreduce.JobContext;
import org.apache.hadoop.mapreduce.RecordReader;
import org.apache.hadoop.mapreduce.TaskAttemptContext;
import org.apache.hadoop.io.NullWritable;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

public class RandomPointInputFormat extends InputFormat<NullWritable, Point2DWritable> {
    @Override
    public List<InputSplit> getSplits(JobContext context) {
        int numSplits = context.getConfiguration().getInt("randompoint.num.splits", 1);
        int pointsPerSplit = context.getConfiguration().getInt("randompoint.points.per.split", 1000);

        List<InputSplit> splits = new ArrayList<>();
        for (int i = 0; i < numSplits; i++) {
            splits.add(new FakeInputSplit(pointsPerSplit));
        }
        return splits;
    }

    @Override
    public RecordReader<NullWritable, Point2DWritable> createRecordReader(InputSplit split, TaskAttemptContext context) {
        return new RandomPointRecordReader();
    }
}
