package DriverPerformanceAnalysis;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;

public class DriverPerformanceMapper extends Mapper<LongWritable, Text, Text, IntWritable> {
    private Text driverName = new Text();
    private IntWritable points = new IntWritable();

    @Override
    protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
        String[] columns = value.toString().split(",");
        // Assuming the driver's name is in the first column and points in the fifth column
        driverName.set(columns[0]);
        points.set(Integer.parseInt(columns[4]));

        context.write(driverName, points);
    }
}
