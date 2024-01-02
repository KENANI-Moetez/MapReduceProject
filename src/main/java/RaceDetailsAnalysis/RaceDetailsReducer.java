package RaceDetailsAnalysis;

import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;

public class RaceDetailsReducer extends Reducer<Text, IntWritable, Text, DoubleWritable> {
    private DoubleWritable averageLapTime = new DoubleWritable();

    @Override
    protected void reduce(Text key, Iterable<IntWritable> values, Context context)
            throws IOException, InterruptedException {
        try {
            int sum = 0;
            int count = 0;

            // Calculate the sum and count of total laps for each race
            for (IntWritable value : values) {
                sum += value.get();
                count++;
            }

            // Calculate the average lap time
            double average = (double) sum / count;
            averageLapTime.set(average);

            context.write(key, averageLapTime);
        } catch (ArithmeticException e) {
            // Log or handle the exception as needed
            System.err.println("Error calculating average in RaceDetailsReducer: " + e.getMessage());
        }
    }
}
