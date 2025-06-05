package bigdata;

import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;

public class RandomPointMapper extends Mapper<NullWritable, Point2DWritable, NullWritable, Point2DWritable> {
    @Override
    protected void map(NullWritable key, Point2DWritable value, Context context) throws IOException, InterruptedException {
        // On transmet simplement le point en sortie
        context.write(NullWritable.get(), value);
    }
}
