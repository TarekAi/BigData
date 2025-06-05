package bigdata;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.util.Map;
import java.util.TreeMap;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

import bigdata.TopK.CityPop;

public class TopK extends Configured implements Tool{
	
	public static class CityPop implements Writable {
		
		public String name;
		public int pop;
		
		public CityPop() {
			this.name = "";
			this.pop = 0;
		}
		
		public CityPop(String name, int pop) {
			this.name = name;
			this.pop = pop;
		}
		
		public void readFields(DataInput in) throws IOException {
			this.name = in.readUTF();
			this.pop = in.readInt();
		}
		
		public void write(DataOutput out) throws IOException {
			out.writeUTF(name);
			out.writeInt(pop);
		}
		
	}

	public static class CityPopMapper extends Mapper<LongWritable,Text,NullWritable, CityPop> {

		private TreeMap<Integer, String> topk = new TreeMap<Integer, String>();
		private int k = 10;

		@Override
		protected void setup(Mapper<LongWritable, Text, NullWritable, CityPop>.Context context)
				throws IOException, InterruptedException {
			this.k = context.getConfiguration().getInt("k", 10);
		}

		@Override
		protected void map(LongWritable key, Text value,
				Mapper<LongWritable, Text, NullWritable, CityPop>.Context context)
				throws IOException, InterruptedException {
			// on retire la premiere ligne (le header)
			if(key.get() == 0L) return;

			String[] tokens = value.toString().split(",");
			// on enleve les villes sans population
			if(tokens[4].isEmpty()) return;
			String cityName = tokens[1];
			int pop = Integer.valueOf(tokens[4]);
			
			// on insere la ville dans le treemap
			topk.put(pop, cityName);
			
			// on retire jusqu'a n'en avoir plus que k
			// normalement effectue une seule fois
			while (topk.size() > k)
				topk.remove(topk.firstKey());
			
		}

		@Override
		protected void cleanup(Mapper<LongWritable, Text, NullWritable, CityPop>.Context context)
				throws IOException, InterruptedException {
			CityPop val = new CityPop();
			//ecrire les k villes restantes
			for(Map.Entry<Integer,String> pair : topk.entrySet()) {
				val.name = pair.getValue();
				val.pop = pair.getKey();
				context.write(NullWritable.get(), val);
			}
		}
	}
	
	public static class TopKCombiner extends Reducer<NullWritable, CityPop, NullWritable, CityPop> {
		private int k = 10;
		
		@Override
		protected void setup(Reducer<NullWritable, CityPop, NullWritable, CityPop>.Context context)
				throws IOException, InterruptedException {
			this.k = context.getConfiguration().getInt("k", 10);
		}

		@Override
		protected void reduce(NullWritable key, Iterable<CityPop> values,
				Reducer<NullWritable, CityPop, NullWritable, CityPop>.Context context)
				throws IOException, InterruptedException {
			TreeMap<Integer, String> topk = new TreeMap<Integer, String>();
			for(CityPop cp : values) {
				//insertion de la ville
				topk.put(cp.pop, cp.name);
				// on conserve les k plus grandes
				while(topk.size() > k) {
					topk.remove(topk.firstKey());
				}
			}
			
			// ecrire les k plus grandes
			for (Map.Entry<Integer, String> v : topk.entrySet()) {
				context.write(NullWritable.get(), new CityPop(v.getValue(), v.getKey()));
			}
		}
	}
	
	public static class TopKReducer extends Reducer<NullWritable, CityPop, Text, Text> {
		
		private int k = 10;
		
		@Override
		protected void setup(Reducer<NullWritable, CityPop, Text, Text>.Context context)
				throws IOException, InterruptedException {
			this.k = context.getConfiguration().getInt("k", 10);
		}

		@Override
		protected void reduce(NullWritable key, Iterable<CityPop> values,
				Reducer<NullWritable, CityPop, Text, Text>.Context context) throws IOException, InterruptedException {
			
			TreeMap<Integer, String> topk = new TreeMap<Integer, String>();
			for(CityPop cp : values) {
				//insertion de la ville
				topk.put(cp.pop, cp.name);
				// on conserve les k plus grandes
				while(topk.size() > k) {
					topk.remove(topk.firstKey());
				}
			}
			
			// ecrire les k plus grandes
			for (Map.Entry<Integer, String> v : topk.entrySet()) {
				context.write(new Text(v.getValue()), new Text(Integer.toString(v.getKey())));
			}
			
		}
		
	}
	
	public int run(String[] args) throws Exception {
		Configuration conf = new Configuration();
		int k = 0;
		k = Integer.parseInt(args[0]);
		conf.setInt("k", k);
		
		Job job = Job.getInstance(conf, "MRTopK");
		job.setNumReduceTasks(1);
		job.setJarByClass(SortedJoiner.class);
		
		job.setInputFormatClass(TextInputFormat.class);
		TextInputFormat.addInputPath(job, new Path(args[1]));
		
		job.setMapperClass(CityPopMapper.class);
		job.setMapOutputKeyClass(NullWritable.class);
		job.setMapOutputValueClass(CityPop.class);
		
		job.setReducerClass(TopKReducer.class);
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(Text.class);
		job.setOutputFormatClass(TextOutputFormat.class);
		
		TextOutputFormat.setOutputPath(job, new Path(args[2]));
		
		return job.waitForCompletion(true) ? 0 : 1;
	}
	

	public static void main(String[] args) throws Exception {
		System.exit(ToolRunner.run(new TopK(), args));
	}

}