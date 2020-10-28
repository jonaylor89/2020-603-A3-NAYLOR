package xyz.jonaylor;

import java.io.IOException;
import java.io.StringReader;
import java.io.StringWriter;
import java.io.FileReader;
import java.io.File;
import java.io.DataInput;
import java.io.DataOutput;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.stream.Stream;
import java.util.StringTokenizer;
import java.util.List;
import java.util.HashMap;
import java.util.Arrays;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.ArrayWritable;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import weka.core.Instances;
import weka.core.Instance;
import weka.core.converters.ArffLoader.ArffReader;

public class KNN {

    public static class PairWritable implements Writable {
        private IntWritable value1;
        private DoubleWritable value2;

        public PairWritable() {
            value1 = new IntWritable();
            value2 = new DoubleWritable();
        }

        public PairWritable(int value1, double value2) {
            this.value1 = new IntWritable(value1);
            this.value2 = new DoubleWritable(value2);
        }

        public int get0() {
            return value1.get();
        }

        public double get1() {
            return value2.get();
        }

        @Override
        public String toString() {
            return value1.toString() + " " + value2.toString();
        }

        @Override
        public void readFields(DataInput in) throws IOException {
            value1.readFields(in);
            value2.readFields(in);
        }

        @Override
        public void write(DataOutput out) throws IOException {
            value1.write(out);
            value2.write(out);
        }
    }

    public static class MapperKNN extends Mapper<Object, Text, IntWritable, ArrayWritable> {

        private Instances testSet;
        private int k;

        public static class Point {
            private double data[];
            private int classValue;
            
            Point(double[] data, int classValue) {
                this.data = data;
                this.classValue = classValue;
            }

            double[] getData() {
                return data;
            }

            int getClassValue() {
                return classValue;
            }
        }

        public static class Tuple implements Comparable<Tuple> {
            private int idx;
            private double distance;

            public Tuple(int idx, double distance) {
                this.idx = idx;
                this.distance = distance;
            }

            public int getIdx() {
                return idx;
            }

            public double getDistance() {
                return distance;
            }

            public int compareTo(Tuple other) {
                return (int)(this.distance - other.getDistance());
            }
        }


        @Override
        public void setup(Context context) throws IOException, InterruptedException {

            Configuration conf = context.getConfiguration();

            k = Integer.parseInt(conf.get("k"));

            String testSetString = conf.get("testInstances");

            if (testSetString == null) {
                System.out.println("oops");
                return;
            }
            
            StringReader reader = new StringReader(testSetString);
            ArffReader arff = new ArffReader(reader);

            testSet = arff.getData();
            testSet.setClassIndex(testSet.numAttributes() - 1);

        }

        private static Point[] parseInput(Text value) {

            String[] dataPoints = value.toString().split("\n");
            Point[] trainSet = new Point[dataPoints.length];

            for (int i = 0; i < dataPoints.length; i++) {
                String[] rawPoint = dataPoints[i].split(",");
                double[] dataPoint = Arrays.asList(rawPoint).stream().mapToDouble(Double::parseDouble).toArray();  

                double[] dimensions = Arrays.copyOfRange(dataPoint, 0, dataPoint.length - 2);
                int classValue = (int)dataPoint[dataPoint.length - 1];
                Point p = new Point(dimensions, classValue);

                trainSet[i] = p;
            }

            return trainSet;
        }

        private Tuple[] getNeighbors(Instance test, Point[] train) {
            
            Tuple[] neighbors = new Tuple[5];
            Tuple[] distances = new Tuple[testSet.numInstances()];
            for (int i = 0; i < train.length; i++) {

                // Get distances
                long squaredSum = 0;
                for (int j = 0; j < testSet.numAttributes() - 2; j++) {
                    squaredSum += Math.pow((double)test.index(j) - train[i].data[j], 2);
                }
                distances[i] = new Tuple(i, Math.sqrt(squaredSum));
            }

            // Sort
            Arrays.sort(distances);

            // Take 5
            for (int i = 0; i < k; i++) {
                neighbors[i] = distances[i];
            }

            return neighbors;
        }

        @Override
        public void map(Object key, Text value, Context context) throws IOException, InterruptedException {
        /*
         *
         * for i in range(testSet)
         *      ComputeKNN(dataPoint, i, TrainSetSplitj, k)
         *      for n in range(0, k)
         *          CDj[i][n] = (class of neighbor n, distance of neighbor n)
         * 
         * key = idMapper * context.write(key, CDj)
         */

         // List<String> allTrainInstances = Arrays.asList(value.toString().split("\n"));
         // System.out.println("allTrainInstances " + allTrainInstances.size());

            Point[] trainSetSplit_j = parseInput(value);

            ArrayWritable CD_j = new ArrayWritable(ArrayWritable.class);
            ArrayWritable[] CD_j_temp = new ArrayWritable[testSet.numInstances()];

            for (int i = 0; i < testSet.numInstances(); i++) {
                Tuple[] neighbors = getNeighbors(testSet.instance(i), trainSetSplit_j);

                ArrayWritable tempWritable = new ArrayWritable(PairWritable.class);
                PairWritable[] temp = new PairWritable[5];

                for (int n = 0; n < k; n++) {
                    int classValue = trainSetSplit_j[neighbors[n].getIdx()].getClassValue();
                    temp[n] = new PairWritable(classValue, neighbors[n].getDistance());
                }

                tempWritable.set(temp);
            }
            CD_j.set(CD_j_temp);

            IntWritable idx = new IntWritable(1);

            context.write(idx, CD_j);
        }
    }

    public static class ReducerKNN extends Reducer<IntWritable, ArrayWritable, IntWritable, IntWritable> {

        private static PairWritable[][] CD_reducer;
        private Instances testSet;
        private int k;

        private int majorityVoting(PairWritable[] row) {

            HashMap<Integer, Integer> histogram = new HashMap<Integer, Integer>();
            int mode_count = 0;
            int mode = -1;
            
            for (int i = 0; i < k; i++) {

                int element = row[i].get0();
                histogram.put(element, histogram.get(element) + 1);

                if (histogram.get(element) > mode_count) {
                    mode_count = histogram.get(element);
                    mode = element;
                }
            }

            return mode;
        }

        @Override
        public void setup(Context context) throws IOException {
        /*
         * 
         * create CD reducer matrix of size (size(testSet * k))
         * 
         * Classes can be random but the distances should be set to infinity
         */

            Configuration conf = context.getConfiguration();

            ArffReader arff = new ArffReader(new StringReader(conf.get("testInstances")));

            testSet = arff.getData();
            testSet.setClassIndex(testSet.numAttributes() - 1);

            k = Integer.parseInt(conf.get("k"));

            CD_reducer = new PairWritable[testSet.numInstances()][k]; 
        }

        @Override
        public void reduce(IntWritable key, Iterable<ArrayWritable> value, Context context) throws IOException, InterruptedException {
        /*
         *
         * for i in range(testSet.size())
         *      cont = 0
         *      for n in range(0, k)
         *          if CDj[i][cont].dist < CD_reducer[i][n].dist
         *              CD_reducer[i][n] = CDj[i][cont]
         *              cont++
         * 
         */

            for (ArrayWritable subset : value) {

                PairWritable[][] CD_j = new PairWritable[testSet.numInstances()][k];

                ArrayWritable[] temp = (ArrayWritable[])subset.toArray();
                for (int j = 0; j < testSet.numInstances(); j++) {
                    CD_j[j] = (PairWritable[])temp[j].toArray();
                }

                for (int i = 0; i < testSet.numInstances(); i++) {
                    int cont = 0;
                    for (int n = 0; n < k; n++) {
                        if (CD_j[i][cont].get1() < CD_reducer[i][n].get1()) {
                            CD_reducer[i][n] = CD_j[i][cont];
                            cont++;
                        }
                    }
                }
            }
        }

        @Override
        public void cleanup(Context context) throws IOException, InterruptedException {
        /*
         *
         * for i in range(0, size(testSet))
         *      PredClass[i] = majorityVoting(classes(CD_reducer))
         *      key = i
         *      context.write(key, PredClass[i])
         *
         */


            for (int i = 0; i < testSet.numInstances(); i++) {
                int prediction = majorityVoting(CD_reducer[i]);
                context.write(new IntWritable(prediction), new IntWritable(prediction));
            }
        }

    }

    private static String getTestInstances(String filename) {

        StringBuilder contentBuilder = new StringBuilder();
 
        try (Stream<String> stream = Files.lines(Paths.get(filename), StandardCharsets.UTF_8)) {
            stream.forEach(s -> contentBuilder.append(s).append("\n"));
        }
        catch (IOException e) {
            e.printStackTrace();
        }

        String contentString = contentBuilder.toString();
 
        return contentBuilder.toString();
    }

    public static void main(String[] argv) throws Exception {

        Configuration conf = new Configuration();

        String testInstances = getTestInstances(argv[2]);
	    conf.set("testInstances", testInstances);

        conf.set("k", argv[3]);

        Job job = Job.getInstance(conf, "KNN");
        job.setJarByClass(KNN.class);

        job.setMapperClass(MapperKNN.class);
        job.setReducerClass(ReducerKNN.class);

        job.setNumReduceTasks(1);

        // job.getConfiguration().setInt(LINES_PER_MAP, 300);

        job.setMapOutputKeyClass(IntWritable.class);
        job.setMapOutputValueClass(ArrayWritable.class);

        job.setOutputKeyClass(IntWritable.class);
        job.setOutputValueClass(IntWritable.class);

        FileInputFormat.addInputPath(job, new Path(argv[0]));
        FileOutputFormat.setOutputPath(job, new Path(argv[1]));

        System.exit(job.waitForCompletion(true) ? 0 : 1);

    }
}
