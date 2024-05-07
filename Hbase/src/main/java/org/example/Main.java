package org.example;

import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;

public class Main  {

    // Mapper
    public static class CryptocurrencyBatchProcessingMapper extends Mapper<LongWritable, Text, Text, CryptocurrencyDataWritable> {

        @Override
        public void map(LongWritable key, Text value, Context context)
                throws IOException, InterruptedException {
            String line = value.toString();
            String[] fields = line.split(","); // Assuming CSV format

            // Extract relevant fields
            String assetID = fields[1];
            // Assuming the CryptocurrencyDataWritable class has been defined to hold necessary data
            CryptocurrencyDataWritable data = new CryptocurrencyDataWritable(
                    Long.parseLong(fields[0]), // timestamp
                    Double.parseDouble(fields[3]), // open
                    Double.parseDouble(fields[4]), // high
                    Double.parseDouble(fields[5]), // low
                    Double.parseDouble(fields[6]), // close
                    Double.parseDouble(fields[7]), // volume
                    Double.parseDouble(fields[8]) // VWAP
            );

            // Emit key-value pair (assetID, CryptocurrencyDataWritable)
            context.write(new Text(assetID), data);
        }
    }

    // Reducer
    public static class CryptocurrencyBatchProcessingReducer extends Reducer<Text, CryptocurrencyDataWritable, Text, Text> {

        @Override
        public void reduce(Text key, Iterable<CryptocurrencyDataWritable> values, Context context)
                throws IOException, InterruptedException {
            // Perform aggregation or analysis tasks
            // Example: Calculate average price and total volume
            double totalPrice = 0.0;
            double totalVolume = 0.0;
            int count = 0;
            for (CryptocurrencyDataWritable data : values) {
                totalPrice += data.getClose(); // Assuming close price is used for calculation
                totalVolume += data.getVolume();
                count++;
            }
            double avgPrice = totalPrice / count;

            // Emit key-value pair (assetID, aggregated data)
            context.write(key, new Text("Average Price: " + avgPrice + ", Total Volume: " + totalVolume));
        }
    }

    // Driver
    public static void main(String[] args) throws Exception {
        // Create a configuration object for Hadoop.
        Configuration conf = new Configuration();

        // Create a Job object: a Hadoop task. Provide the Hadoop configuration and a textual description of the task.
        Job job = Job.getInstance(conf, "CryptocurrencyBatchProcessing");

        // Indicate to the job the relevant classes: driver, mapper, and reducer.
        job.setJarByClass(Main.class);
        job.setMapperClass(CryptocurrencyBatchProcessingMapper.class);
        job.setReducerClass(CryptocurrencyBatchProcessingReducer.class);

        // Define the key/value types for our Hadoop program.
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(CryptocurrencyDataWritable.class);

        // Set the input and output formats.
        job.setInputFormatClass(TextInputFormat.class);
        job.setOutputFormatClass(TextOutputFormat.class);

        // Set the input and output paths from the command-line arguments.
        TextInputFormat.addInputPath(job, new Path(args[0]));
        TextOutputFormat.setOutputPath(job, new Path(args[1]));

        // Run the Hadoop task. If it completes successfully, exit with 0. Otherwise, exit with -1.
        System.exit(job.waitForCompletion(true) ? 0 : 1);
    }
}
