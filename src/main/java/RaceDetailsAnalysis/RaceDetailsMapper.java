package RaceDetailsAnalysis;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;

public class RaceDetailsMapper extends Mapper<LongWritable, Text, Text, IntWritable> {
    private Text raceName = new Text();
    private IntWritable totalLaps = new IntWritable();

    @Override
    protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
        try {
            String[] columns = value.toString().split(",");

            // Assuming the race name is in the second column and total laps in the fifth column
            raceName.set(columns[1]);
            totalLaps.set(Integer.parseInt(columns[4]));

            context.write(raceName, totalLaps);
        } catch (NumberFormatException | ArrayIndexOutOfBoundsException e) {
            // Log or handle the exception as needed
            System.err.println("Error in RaceDetailsMapper: " + e.getMessage());
        }
    }
}
