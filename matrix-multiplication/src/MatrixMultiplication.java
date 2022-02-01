package matrixmultiplication;

import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

public class MatrixMultiplication {
	public static void main(String[] args) throws IOException {
		Configuration conf = new Configuration();
		Job job = Job.getInstance(conf, "JobName");

		try {

			deleteFolder(conf, Constants.outputFilePath);
			deleteFolder(conf, Constants.finalResultPath);
			

			myMCRTask(job);
			
			new MatrixCProcessing().getMatrixC(
					Constants.outputFilePath + "/part-r-00000", Constants.finalResultPath);
			
			System.out.println("OPERATION SUCCESSFUL");
			System.out.println("FINAL OUTPUT WRITTEN TO FILE: " + Constants.finalResultPath);
		} catch(Exception e) {
			System.out.println("Cannot Continue ! Some Issue");
			System.out.println("Reason => " + e.getMessage());
		}
	}

	private static void myMCRTask(Job job)
			throws IllegalArgumentException,
			IOException,
			ClassNotFoundException,
			InterruptedException {
		job.setJarByClass(matrixmultiplication.MatrixMultiplication.class);


		job.setMapperClass(MatrixMultiplicationMapper.class);
		job.setCombinerClass(MatrixMultiplicationCombiner.class);
		job.setReducerClass(MatrixMultiplicationReducer.class);

		job.setMapOutputKeyClass(Text.class);
		job.setMapOutputValueClass(Text.class);


		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(IntWritable.class);

		FileInputFormat.setInputPaths(job, new Path(Constants.inputFilePath));
		FileOutputFormat.setOutputPath(job, new Path(Constants.outputFilePath));

		if (!job.waitForCompletion(true))
			return;

	}

	private static void deleteFolder(Configuration conf, String folderPath) throws IOException {
		FileSystem fs = FileSystem.get(conf);
		Path path = new Path(folderPath);
		if(fs.exists(path)) {
			fs.delete(path,true);
		}
	}
}