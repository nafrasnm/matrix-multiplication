package matrixmultiplication;

import java.io.IOException;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

public class MatrixMultiplicationReducer extends
Reducer<Text, Text, Text, IntWritable> {

	public void reduce(Text key, Iterable<Text> values, Context context)
			throws IOException, InterruptedException {
		
		int sum = 0;

		for (Text val : values) {
			sum += Integer.parseInt(val.toString());
		}

		System.out.println("Reducer Emiting : <" + key + "," + sum + ">");
		context.write(key, new IntWritable(sum));
	}
}