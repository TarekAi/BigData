package bigdata;

import java.io.IOException;
import java.util.LinkedList;
import java.util.List;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.MultipleInputs;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

public class Joiner extends Configured implements Tool {
	
	public static class JoinMapperCities
	extends Mapper<LongWritable, Text, Text, TaggedValue>{
		
		@Override
		public void map(LongWritable key, Text value, Context context
				) throws IOException, InterruptedException {
			// on retire la premiere ligne (le header)
			if(key.get() == 0L) return;
			
			String[] tokens = value.toString().split(",");
			String country = tokens[0].toLowerCase();
			String regCode = tokens[3];
			String cityName = tokens[1];
			
			String k = country+","+regCode;
			
			context.write(new Text(k), new TaggedValue(cityName, true));
		}
	}
	
	public static class JoinMapperRegions
	extends Mapper<LongWritable, Text, Text, TaggedValue>{
		
		@Override
		public void map(LongWritable key, Text value, Context context
				) throws IOException, InterruptedException {
			// pas de header dans le fichier des regions
			
			String[] tokens = value.toString().split(",");
			String country = tokens[0].toLowerCase();
			String regCode = tokens[1];
			String regName = tokens[2];
			
			String k = country+","+regCode;
			
			context.write(new Text(k), new TaggedValue(regName, false));
		}
	}
	
	public static class JoinReducer
	extends Reducer<Text,TaggedValue,NullWritable,Text> {
		
		@Override
		public void reduce(Text key, Iterable<TaggedValue> values, Context context) throws IOException, InterruptedException {
			// liste des villes en attente de join
			List<String> cities = new LinkedList<String>();
			// la region (null par defaut)
			String region = null;
			
			// on lit chaque valeur
			for (TaggedValue v : values) {
				
				// si c'est une region...
				if(!v.isCity) {
					// ... on donne son nom a la region
					region = v.name;
					
					// on fait le join sur les villes en attente
					for (String city : cities) {
						context.write(NullWritable.get(), new Text(city + "," + region));
					}
					// et on vide la liste de villes en attente
					cities.clear();
				}
				// sinon, c'est que c'et une ville
				else {
					if(region != null)
						//on connait la region, donc on fait le join
						context.write(NullWritable.get(), new Text(v.name + "," + region));
					else
						//on a pas encore lu la region, donc on met la ville en attente
						cities.add(v.name);
				}
			}
			
			// plus de valeurs a lire
			// mais il se peut qu'on ait pas vu passer de region
			// (certains pays n'ont pas d'information de region)
			if(region == null) {
				// dans ce cas, on fait un join avec une chaine vide en guise de nom de region
				region = "";
				for (String city : cities) {
					context.write(NullWritable.get(), new Text(city + "," + region));
				}
				cities.clear();
			}
		}
	}
	
	public int run(String[] args) throws Exception {
		Configuration conf = new Configuration();
		Job job = Job.getInstance(conf, "MRJoin");
		job.setNumReduceTasks(1);
		job.setJarByClass(Joiner.class);
		
		MultipleInputs.addInputPath(job, new Path(args[0]), TextInputFormat.class, JoinMapperCities.class);
		MultipleInputs.addInputPath(job, new Path(args[1]), TextInputFormat.class, JoinMapperRegions.class);
		
		job.setMapOutputKeyClass(Text.class);
		job.setMapOutputValueClass(TaggedValue.class);
		job.setReducerClass(JoinReducer.class);
		job.setOutputKeyClass(NullWritable.class);
		job.setOutputValueClass(Text.class);
		job.setOutputFormatClass(TextOutputFormat.class);
		
		TextOutputFormat.setOutputPath(job, new Path(args[2]));
		
		return job.waitForCompletion(true) ? 0 : 1;
	}
	

	public static void main(String[] args) throws Exception {
		System.exit(ToolRunner.run(new Joiner(), args));
	}

}