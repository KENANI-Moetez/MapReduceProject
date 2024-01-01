package FastestLapAnalysis;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;

public class FastestLapReducer extends Reducer<Text, IntWritable, Text, IntWritable> {
    private IntWritable totalFastestLaps = new IntWritable();

    @Override
    protected void reduce(Text key, Iterable<IntWritable> values, Context context)
            throws IOException, InterruptedException {
        int sum = 0;

        // Calculate the total number of fastest laps for each driver
        for (IntWritable value : values) {
            sum += value.get();
        }

        totalFastestLaps.set(sum);

        context.write(key, totalFastestLaps);
    }
}
