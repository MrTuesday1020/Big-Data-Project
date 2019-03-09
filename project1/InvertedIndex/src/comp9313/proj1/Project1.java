package comp9313.proj1;

import java.io.IOException;
import java.util.HashMap;
import java.util.StringTokenizer;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Partitioner;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

public class Project1 {
	
	public static class TFIDFMapper extends Mapper<Object, Text, Text, DoubleWritable> {

		private Text text = new Text();
		private static DoubleWritable result = new DoubleWritable(1.0);
		private static Double line_number = 0.0;
		HashMap<String, Double> hm = new HashMap<String, Double>();

		public void map(Object key, Text value, Context context) throws IOException, InterruptedException {
			
			StringTokenizer itr = new StringTokenizer(value.toString());
			
			String docid = itr.nextToken();
			
			line_number = line_number + 1.0;
			
			while (itr.hasMoreTokens()) {
				String word = itr.nextToken().toLowerCase();
				// normal key is term and document id 
				String normal_key = word + "\t" + docid;
				// special key is term and document id 
				String special_key = word + "\t*";
				// add special key and value into result set
				if (!hm.containsKey(normal_key)){
					hm.put(normal_key, 1.0);
					// add special key and value into result set
					if (!hm.containsKey(special_key)){
						hm.put(special_key, 1.0);
					}
					else{
						hm.put(special_key, hm.get(special_key) + 1.0);
					}
				}
				else{
					hm.put(normal_key, hm.get(normal_key) + 1.0);
				}
			}
		}
		
		@Override
		protected void cleanup(Mapper<Object, Text, Text, DoubleWritable>.Context context) throws IOException, InterruptedException {
			// output the results together
			for(String this_key : hm.keySet()){
				text.set(this_key + "\t" + line_number.toString());
				result.set(hm.get(this_key));
				context.write(text, result);
			};
		}
	}
	
	public static class TFIDFPartitioner extends Partitioner<Text, DoubleWritable>{

		@Override
		public int getPartition(Text key, DoubleWritable value, int NumOfReducer) {
			StringTokenizer itr = new StringTokenizer(key.toString());
			
			char first_letter = itr.nextToken().charAt(0);
			// dispatch records to different reducers according the first letter of the keys 
			if (first_letter < 'i'){
				return 0;
			} else if ( first_letter > 'r'){
				return 2;
			} else {
				return 1;
			}
		}
	}
	
	public static class TFIDFReducer extends Reducer<Text, DoubleWritable, Text, DoubleWritable> {
		
		private Text text = new Text();
		private static DoubleWritable result = new DoubleWritable();
		private double n; // the number of docs with the term
		private double tf; // tf value
		private double df; // df value
		private double tfidf; // tfidf value
		HashMap<String, Double> hm = new HashMap<String, Double>();
		
		public void reduce(Text key, Iterable<DoubleWritable> values, Context context) throws IOException, InterruptedException {
			
			StringTokenizer itr = new StringTokenizer(key.toString());
			
			double sum = 0.0;
			// get the word
			String word = itr.nextToken();
			// get the number of docs with the term
			String docid = itr.nextToken();
			// get the number of docs in entire collection
			double N = Double.parseDouble(itr.nextToken());
			if (docid.equals("*")){
				for (DoubleWritable value : values) {
					sum += value.get();
				}
				n = sum;
			} else {
				for (DoubleWritable value : values) {
					sum += value.get();
				}
				// compute tf and df separately
				tf = sum;
				df = N / n;
				// compute tfidf
				tfidf = tf * Math.log10(df);
				// set key and value
				text.set(word + "\t" + docid);
				result.set(tfidf);
				// output
				context.write(text, result);
			}
		}
	}
	
	public static void main(String[] args) throws Exception {
		Configuration conf = new Configuration();
		conf.set("mapred.textoutputformat.separator", ",");
		Job job = Job.getInstance(conf, "Project1");
		job.setJarByClass(Project1.class);
		job.setMapperClass(TFIDFMapper.class);
		job.setPartitionerClass(TFIDFPartitioner.class);
		job.setReducerClass(TFIDFReducer.class);
		job.setNumReduceTasks(3);
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(DoubleWritable.class);
		FileInputFormat.addInputPath(job, new Path(args[0]));
		FileOutputFormat.setOutputPath(job, new Path(args[1]));
		job.waitForCompletion(true);
		System.exit(0);
	}
}
