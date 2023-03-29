package mirko.evcount;

import java.io.IOException;
import java.util.Comparator;
import java.util.PriorityQueue;
import java.util.AbstractMap.SimpleEntry;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.output.MultipleOutputs;

public class EVC_Reducer extends Reducer<Text, IntWritable, Text, IntWritable> {
	private int N;
	private PriorityQueue<SimpleEntry<Text, IntWritable>> top;
	private PriorityQueue<SimpleEntry<Text, IntWritable>> low;
	private MultipleOutputs<Text, IntWritable> mos;

	protected void setup(Context context) throws IOException, InterruptedException {
		N = Integer.parseInt(context.getConfiguration().get("NumberOfElements"));
		top = new PriorityQueue<SimpleEntry<Text, IntWritable>>(N, new Comparator<SimpleEntry<Text, IntWritable>>() {
			public int compare(SimpleEntry<Text, IntWritable> t1, SimpleEntry<Text, IntWritable> t2) {
				return t1.getValue().compareTo(t2.getValue());
			}
			
		});

		low = new PriorityQueue<SimpleEntry<Text, IntWritable>>(N, new Comparator<SimpleEntry<Text, IntWritable>>() {
			public int compare(SimpleEntry<Text, IntWritable> t1, SimpleEntry<Text, IntWritable> t2) {
				return -(t1.getValue().compareTo(t2.getValue()));
			}
		});

		mos = new MultipleOutputs<Text, IntWritable>(context);
	}
	
	public void reduce(Text key, Iterable<IntWritable> values, Context context)
			throws IOException, InterruptedException {

		int sum = 0;

		for (IntWritable value : values) {
			sum += value.get();
		}

		SimpleEntry<Text, IntWritable> entry = new SimpleEntry<>(
			new Text(key), new IntWritable(sum)
		)
		;
		top.add(entry);
		if (top.size() > N) {
			top.remove();
		}

		low.add(entry);
		if (low.size() > N) {
			low.remove();
		}
	}

	public void cleanup(Context context) throws IOException, InterruptedException {
		for(SimpleEntry<Text, IntWritable> entry : top) {
			mos.write("top", entry.getKey(), entry.getValue());
		}

		for(SimpleEntry<Text, IntWritable> entry : low) {
			mos.write("low", entry.getKey(), entry.getValue());
		}
	}
}
