package bigdata;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.output.SequenceFileOutputFormat;

public class TP3 {
    public static void main(String[] args) throws Exception {
        if (args.length < 3) {
            System.err.println("Usage: TP3 <output path> <num splits> <points per split>");
            System.exit(-1);
        }

        Configuration conf = new Configuration();

        // Paramètres pour le input format
        conf.setInt("randompoint.num.splits", Integer.parseInt(args[1]));
        conf.setInt("randompoint.points.per.split", Integer.parseInt(args[2]));

        Job job = Job.getInstance(conf, "Random Point Generator");
        job.setJarByClass(TP3.class);

        // Input format personnalisé
        job.setInputFormatClass(RandomPointInputFormat.class);

        // Mapper & Reducer
        job.setMapperClass(RandomPointMapper.class);
        job.setReducerClass(RandomPointReducer.class);

        // Output key/value classes
        job.setMapOutputKeyClass(NullWritable.class);
        job.setMapOutputValueClass(Point2DWritable.class);
        job.setOutputKeyClass(NullWritable.class);
        job.setOutputValueClass(Point2DWritable.class);

        // Output format personnalisé
        job.setOutputFormatClass(SequenceFileOutputFormat.class);

        // Output path (SequenceFileOutputFormat)
        SequenceFileOutputFormat.setOutputPath(job, new Path(args[0]));
        // On fixe le nombre de reduceurs à 1 (par défaut)
        job.setNumReduceTasks(1);

        System.exit(job.waitForCompletion(true) ? 0 : 1);
    }
}
