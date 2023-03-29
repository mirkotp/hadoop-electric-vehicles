package mirko.evcount;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

public class EVC_Reducer extends Reducer<Text, IntWritable, Text, IntWritable> {
	class KV implements Comparable<KV> {
		Text key;
		int value;

		KV(Text key, int value) {
			this.key = new Text(key);
			this.value = value;
		}

		public int compareTo(KV kv) {
			if (value < kv.value) return -1;
			else if (value > kv.value) return 1;
			else return 0;
		}
	}

	List<KV> kvs = new ArrayList<KV>();
	
	public void reduce(Text key, Iterable<IntWritable> values, Context context)
			throws IOException, InterruptedException {

		int sum = 0;

		for (IntWritable value : values) {
			sum += value.get();
		}

		kvs.add(new KV(key, sum));
	}

	public void cleanup(Context context) throws IOException, InterruptedException {
		final int N = Integer.parseInt(context.getConfiguration().get("NumberOfElements"));

		KV[] arr = kvs.stream().sorted().toArray(KV[]::new);
		int size = arr.length;

		if (size <= 2*N) {
			for (KV kv : arr) {
				context.write(kv.key, new IntWritable(kv.value));
			}
		} else {
			for (int i = 0; i < N; i++) {
				context.write(arr[i].key, new IntWritable(arr[i].value));
			}

			for (int i = size - N; i < size; i++) {
				context.write(arr[i].key, new IntWritable(arr[i].value));
			}
		}
	}
}
