package DriverPerformanceAnalysis;

import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;

public class DriverPerformanceReducer extends Reducer<Text, IntWritable, Text, DoubleWritable> {
    private DoubleWritable averagePosition = new DoubleWritable();

    @Override
    protected void reduce(Text key, Iterable<IntWritable> values, Context context)
            throws IOException, InterruptedException {
        int sum = 0;
        int count = 0;

        // Calculate the sum and count of points for each driver
        for (IntWritable value : values) {
            sum += value.get();
            count++;
        }

        // Calculate the average position
        double average = (double) sum / count;
        averagePosition.set(average);

        context.write(key, averagePosition);
    }
}
