import java.io.IOException;
import java.util.HashMap;
import java.util.Map;
import java.util.StringTokenizer;
import java.util.TreeMap;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.FloatWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;

public class ProfitableMonthSeason {
    // Mapper class
    public static class BookingMapper extends Mapper<Object, Text, Text, FloatWritable> {
        private final static FloatWritable revenue = new FloatWritable();
        private final Text season = new Text();

        // Map method that processes one line at a time, as provided by the specified TextInputFormat
        public void map(Object key, Text value, Context context) throws IOException, InterruptedException {
            // Parse the input record and extract the necessary attributes
            String[] attributes = value.toString().split(",");
            String season_str = attributes[2];
            int bookingStatus = Integer.parseInt(attributes[0]);
            float totalCost = Float.parseFloat(attributes[1]);

            // Filter out canceled bookings
            if (bookingStatus == 0) {
                // Setting season as key and revenue as value
                season.set(season_str);
                revenue.set(totalCost);
                context.write(season, revenue);
            }
        }
    }
    // Reducer class
    public static class BookingReducer extends Reducer<Text, FloatWritable, Text, FloatWritable> {
        // TreeMap used for sorting
        private TreeMap<Float, Text> revenueMap = new TreeMap<>((a, b) -> -1 * a.compareTo(b));

        // Reduce method that takes the input values, processes them, and outputs the reduced value
        public void reduce(Text key, Iterable<FloatWritable> values, Context context)
                throws IOException, InterruptedException {
            float totalRevenue = 0;
            // Calculate the total revenue for each season
            for (FloatWritable val : values) {
                totalRevenue += val.get();
            }
            // Add revenue and season to the TreeMap for sorting
            revenueMap.put(totalRevenue, new Text(key.toString()));
        }
        // Cleanup method that is run once after all calls to the reduce method for a particular key
        protected void cleanup(Context context) throws IOException, InterruptedException {
            // Output the sorted results
            for (Map.Entry<Float, Text> entry : revenueMap.entrySet()) {
                float revenue = entry.getKey();
                Text season = entry.getValue();
                context.write(season, new FloatWritable(revenue));
            }
        }
    }
    // Driver code - where the job's configurations are set
    public static void main(String[] args) throws Exception {
        Configuration conf = new Configuration();
        // Create a Job instance
        Job job = Job.getInstance(conf, "Hotel Booking Profitability");
        // Set the Jar by finding where a given class came from
        job.setJarByClass(ProfitableMonthSeason.class);
        // Set the Mapper and Reducer class for the job
        job.setMapperClass(BookingMapper.class);
        job.setReducerClass(BookingReducer.class);
        // Set the output key and value class for the job
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(FloatWritable.class);
        // Set the OutputFormat for the job to be TextOutputFormat
        job.setOutputFormatClass(TextOutputFormat.class);
        // Set the input and output file paths
        FileInputFormat.addInputPath(job, new Path(args[0]));
        FileOutputFormat.setOutputPath(job, new Path(args[1]));
        // Start the job, and wait for it to complete
        System.exit(job.waitForCompletion(true) ? 0 : 1);
    }
}

