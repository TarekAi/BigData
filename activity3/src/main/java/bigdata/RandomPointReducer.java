package bigdata;

import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;

public class RandomPointReducer extends Reducer<NullWritable, Point2DWritable, NullWritable, Point2DWritable> {
    @Override
    protected void reduce(NullWritable key, Iterable<Point2DWritable> values, Context context) throws IOException, InterruptedException {
        for (Point2DWritable point : values) {
            context.write(NullWritable.get(), point);
        }
    }
}
