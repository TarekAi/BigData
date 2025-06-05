package bigdata;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.SequenceFileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;

public class PiEstimation {

    public static void main(String[] args) throws Exception {
        if (args.length != 2) {
            System.err.println("Usage: PiEstimation <input path> <output path>");
            System.exit(-1);
        }

        String inputPath = args[0];
        String outputPath = args[1];

        Configuration conf = new Configuration();

        Job job = Job.getInstance(conf, "Pi Estimation");
        job.setJarByClass(PiEstimation.class);

        job.setMapperClass(PiMapper.class);
        job.setReducerClass(PiReducer.class);

        job.setInputFormatClass(SequenceFileInputFormat.class); // car points générés stockés en sequence file
        job.setOutputFormatClass(TextOutputFormat.class);

        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(IntWritable.class);

        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(IntWritable.class);

        SequenceFileInputFormat.addInputPath(job, new Path(inputPath));
        TextOutputFormat.setOutputPath(job, new Path(outputPath));

        boolean success = job.waitForCompletion(true);

        if (success) {
            // Après la fin du job, lire le résultat et calculer pi approximé
            System.out.println("Job terminé. Résultats dans " + outputPath);
        }

        Path resultFile = new Path(outputPath + "/part-r-00000");
        org.apache.hadoop.fs.FileSystem fs = resultFile.getFileSystem(conf);
        org.apache.hadoop.fs.FSDataInputStream inputStream = fs.open(resultFile);

        java.io.BufferedReader reader = new java.io.BufferedReader(new java.io.InputStreamReader(inputStream));

        int inside = 0;
        int outside = 0;
        String line;
        while ((line = reader.readLine()) != null) {
            String[] parts = line.split("\\s+");
            if (parts[0].equals("inside")) {
                inside = Integer.parseInt(parts[1]);
            } else if (parts[0].equals("outside")) {
                outside = Integer.parseInt(parts[1]);
            }
        }
        reader.close();

        int total = inside + outside;
        if (total > 0) {
            double pi = 4.0 * inside / total;
            System.out.println("Approximation de π : " + pi);
        } else {
            System.out.println("Erreur : nombre total de points nul.");
        }

        System.exit(success ? 0 : 1);
    }
}
