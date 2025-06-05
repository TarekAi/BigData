package bigdata;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;

public class PiMapper extends Mapper<NullWritable, Point2DWritable, Text, IntWritable> {

    private final static Text insideKey = new Text("inside");
    private final static Text outsideKey = new Text("outside");
    private final static IntWritable one = new IntWritable(1);

    @Override
    protected void map(NullWritable key, Point2DWritable value, Context context) throws IOException, InterruptedException {
        double x = value.getPoint().getX();
        double y = value.getPoint().getY();

        if (x * x + y * y <= 1.0) {
            context.write(insideKey, one);
        } else {
            context.write(outsideKey, one);
        }
    }
}
