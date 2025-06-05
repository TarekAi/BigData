package bigdata;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.util.Iterator;
import java.util.LinkedList;
import java.util.List;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.WritableComparable;
import org.apache.hadoop.io.WritableComparator;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Partitioner;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.MultipleInputs;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

public class SortedJoiner extends Configured implements Tool {
	
	public static class TaggedKey implements WritableComparable<TaggedKey> {

		//cle naturelle, c a d la cle utilisee precedement
		public String naturalKey;
		//indique s'il s'agit d'une ville ou d'une region
		public boolean isCity;
		
		public TaggedKey() {
			
		}
		
		public TaggedKey(String key, boolean isCity) {
			this.naturalKey = key;
			this.isCity = isCity;
		}

		public void readFields(DataInput in) throws IOException {
			this.naturalKey=in.readUTF();
			this.isCity = in.readBoolean();
		}

		public void write(DataOutput out) throws IOException {
			out.writeUTF(naturalKey);
			out.writeBoolean(isCity);
		}

		public int compareTo(TaggedKey o) {
			//comparaison des cle
			int keyCompare = this.naturalKey.compareTo(o.naturalKey);
			if(keyCompare != 0) {
				// si les cles sont differentes, on utilise cette comparaison
				return keyCompare;
			} else {
				// sinon, on utilise le type (ville ou region) pour les comparer
				// pour Boolean.compare, false vient avant true,
				// donc les regions sont placées avant les villes
				return Boolean.compare(this.isCity, o.isCity);
			}
			
		}
		
	}
	
	public static class JoinMapperCities
	extends Mapper<LongWritable, Text, TaggedKey, TaggedValue>{
		
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
			
			context.write(new TaggedKey(k, true), new TaggedValue(cityName, true));
		}
	}
	
	public static class JoinMapperRegions
	extends Mapper<LongWritable, Text, TaggedKey, TaggedValue>{
		
		@Override
		public void map(LongWritable key, Text value, Context context
				) throws IOException, InterruptedException {
			// pas de header dans le fichier des regions
			
			String[] tokens = value.toString().split(",");
			String country = tokens[0].toLowerCase();
			String regCode = tokens[1];
			String regName = tokens[2];
			
			String k = country+","+regCode;
			
			context.write(new TaggedKey(k, false), new TaggedValue(regName, false));
		}
	}
	
	public static class JoinPartitioner extends Partitioner<TaggedKey, TaggedValue> {

		@Override
		public int getPartition(TaggedKey k, TaggedValue v, int numPartitions) {
			// String.hashCode() peut renvoyer une valeur negative
			// Dans ce cas, l'operateur % donne une valeur negative
			// On utilise donc Math.abs pour eviter cela
			return Math.abs(k.naturalKey.hashCode() % numPartitions);
		}
		
	}
	
	public static class JoinGrouping extends WritableComparator {

		public JoinGrouping() {
			// evite l'appel au constructeur par defaut,
			// qui provoque une NullPointerException
			super(TaggedKey.class, true);
		}

		@Override
		public int compare(WritableComparable a, WritableComparable b) {
			TaggedKey ka = (TaggedKey)a;
			TaggedKey kb = (TaggedKey)b;
			// les cles sont identiques si la premiere partie est identique
			return ka.naturalKey.compareTo(kb.naturalKey);
		}		
	}
	
	public static class JoinSort extends WritableComparator {

		public JoinSort() {
			// evite l'appel au custructeur par defaut,
			// qui provoque une NullPointerException
			super(TaggedKey.class, true);
		}

		@Override
		public int compare(WritableComparable a, WritableComparable b) {
			TaggedKey ka = (TaggedKey)a;
			TaggedKey kb = (TaggedKey)b;
			// on utilise simplement la fonction de comparaison de TaggedKey
			return ka.compareTo(kb);
		}		
	}
	
	public static class JoinReducer
	extends Reducer<TaggedKey,TaggedValue,NullWritable,Text> {
		
		@Override
		public void reduce(TaggedKey key, Iterable<TaggedValue> values, Context context) throws IOException, InterruptedException {
			Iterator<TaggedValue> it = values.iterator();
			TaggedValue first = it.next();
			String region = "";
			if(!first.isCity) {
				region = first.name;
			} else {
				context.write(NullWritable.get(), new Text(first.name+","+region));
			}
			while(it.hasNext()) {
				String city = it.next().name;
				context.write(NullWritable.get(), new Text(city+","+region));
			}
		}
	}
	
	public int run(String[] args) throws Exception {
		Configuration conf = new Configuration();
		Job job = Job.getInstance(conf, "MRSortedJoin");
		job.setNumReduceTasks(5);
		job.setJarByClass(SortedJoiner.class);
		
		MultipleInputs.addInputPath(job, new Path(args[0]), TextInputFormat.class, JoinMapperCities.class);
		MultipleInputs.addInputPath(job, new Path(args[1]), TextInputFormat.class, JoinMapperRegions.class);
		
		job.setMapOutputKeyClass(TaggedKey.class);
		job.setMapOutputValueClass(TaggedValue.class);
		
		job.setPartitionerClass(JoinPartitioner.class);
		job.setGroupingComparatorClass(JoinGrouping.class);
		job.setSortComparatorClass(JoinSort.class);
		
		job.setReducerClass(JoinReducer.class);
		job.setOutputKeyClass(NullWritable.class);
		job.setOutputValueClass(Text.class);
		job.setOutputFormatClass(TextOutputFormat.class);
		
		TextOutputFormat.setOutputPath(job, new Path(args[2]));
		
		return job.waitForCompletion(true) ? 0 : 1;
	}
	

	public static void main(String[] args) throws Exception {
		System.exit(ToolRunner.run(new SortedJoiner(), args));
	}

}