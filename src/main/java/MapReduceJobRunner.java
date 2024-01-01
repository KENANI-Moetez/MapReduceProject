import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;

public class MapReduceJobRunner {

    public static void main(String[] args) throws Exception {
        // Create a Hadoop configuration
        Configuration conf = new Configuration();

        // Run Driver Performance Analysis
        runMapReduceJob(conf, "DriverPerformanceAnalysis",
                DriverPerformanceAnalysis.DriverPerformanceMapper.class,
                DriverPerformanceAnalysis.DriverPerformanceReducer.class,
                "/user/hduser/input/GrandPrix_drivers_details_1950_to_2022.csv",
                "/user/hduser/output/driver_performance_analysis");

        // Run Fastest Laps Analysis
        runMapReduceJob(conf, "FastestLapsAnalysis",
                FastestLapAnalysis.FastestLapMapper.class,
                FastestLapAnalysis.FastestLapReducer.class,
                "/user/hduser/input/GrandPrix_fastest-laps_details_1950_to_2022.csv",
                "/user/hduser/output/fastest_laps_analysis");

        // Run Races Details Analysis
        runMapReduceJob(conf, "RacesDetailsAnalysis",
                RaceDetailsAnalysis.RaceDetailsMapper.class,
                RaceDetailsAnalysis.RaceDetailsReducer.class,
                "/user/hduser/input/GrandPrix_races_details_1950_to_2022.csv",
                "/user/hduser/output/races_details_analysis");
    }

    private static void runMapReduceJob(Configuration conf, String jobName,
                                        Class<? extends org.apache.hadoop.mapreduce.Mapper> mapperClass,
                                        Class<? extends org.apache.hadoop.mapreduce.Reducer> reducerClass,
                                        String inputPath, String outputPath) throws Exception {
        // Create a new MapReduce job
        Job job = Job.getInstance(conf, jobName);

        // Set the main class for the JAR file
        job.setJarByClass(MapReduceJobRunner.class);

        // Choose the appropriate Mapper and Reducer classes based on the dataset
        job.setMapperClass(mapperClass);
        job.setReducerClass(reducerClass);

        // Set input and output formats
        job.setInputFormatClass(TextInputFormat.class);
        job.setOutputFormatClass(TextOutputFormat.class);

        // Set input and output paths
        FileInputFormat.addInputPath(job, new Path(inputPath));
        TextOutputFormat.setOutputPath(job, new Path(outputPath));

        // Specify the key and value types for Mapper and Reducer
        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(IntWritable.class);

        // Specify the final output key and value types
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(DoubleWritable.class);

        // Wait for the job to complete and print progress information
        if (job.waitForCompletion(true)) {
            System.out.println(jobName + " Job completed successfully!");
        } else {
            System.out.println(jobName + " Job failed!");
        }
    }
}
