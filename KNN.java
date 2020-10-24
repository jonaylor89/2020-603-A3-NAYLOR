package xyz.jonaylor;

import java.io.IOException;
import java.io.BufferedReader;
import java.io.FileReader;
import java.io.File;
import java.util.StringTokenizer;

import org.apache.hadoop.commons.io.FilesUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.IntWritable;
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
import weka.core.ArffReader;

public class KNN {

    private static int k;
    private static Instances testSet;

    public class PairWritable implements Writable {
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

        public int get1() {
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

    public static class MapperKNN extends Mapper<Object, Text, IntWritable, PairWritable> {

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

        private static void computeKNN(Instance test, Point[] train) {
            
        }

        @Override
        public static void map(Object key, Text value, Context context) throws IOException {
        /*
         *
         * for i in range(testSet)
         *      ComputeKNN(dataPoint, i, TrainSetSplitj, k)
         *      for n in range(0, k)
         *          CDj[i][n] = (class of neighbor n, distance of neighbor n)
         * 
         * key = idMapper
         * context.write(key, CDj)
         */

         /*
            PairWritable[][] CD_j = new double[testSet.numInstances()][k];

            StringTokenizer tokenizer = new StringTokenizer(value, ",");

            for (int i = 0; i < testSet.numInstances(); i++) {
                computeKNN(testSet[i], trainSetSplit_j);

                for (int n = 0; n < k; n++) {
                    CD_j[i][n] = new PairWritable(testSet[i].getClassValue(), distance);
                }
            }

            context.write(1, CD_j);
         */
        }
    }

    public static class ReducerKNN extends Reducer<IntWritable, PairWritable, IntWritable, IntWritable> {

        private static PairWritable CD_reducer[][];

        private static int majorityVoting(PairWritable[] row) {

            HashMap<Integer, Integer> histogram = new HashMap<Integer, Integer>();
            int mode_count = 0;
            int mode = -1;
            
            for (int i = 0; i < k; i++) {

                int element = row[i].get0();
                histogram[element]++;

                if (histogram[element] > mode_count) {
                    mode_count = histogram[element];
                    mode = element;
                }
            }

            return mode;
        }

        @Override
        public static void setup(Context context) throws IOException {
        /*
         * 
         * create CD reducer matrix of size (size(testSet * k))
         * 
         * Classes can be random but the distances should be set to infinity
         */

            CD_reducer = new PairWritable[testSet.numInstances()][k]; 

        }

        @Override
        public static void reduce(IntWritable key, PairWritable[][] value, Context context) throws IOException {
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

            for (int i = 0; i < testSet.numInstances(); i++) {
                int cont = 0;
                for (int n = 0; n < k; n++) {
                    if (value[i][cont].get1() < CD_reducer[i][n].get1()) {
                        CD_reducer[i][n] = value[i][cont];
                        cont++;
                    }
                }
            }
        }

        @Override
        public static void cleanup(Context context) throws IOException {
        /*
         *
         * for i in range(0, size(testSet))
         *      PredClass[i] = majorityVoting(classes(CD_reducer))
         *      key = i
         *      context.write(key, PredClass[i])
         *
         */

            for (int i = 0; i < testSet.numInstances(); i++) {
                prediction = majorityVoting(CD_reducer[i]);
                context.write(i, prediction);
            }
        }

    }

    public static void main(String[] argv) {

        k = 5;

        BufferedReader reader = new BufferedReader(new FileReader("./Test/small.arff"));
        ArffReader arff = new ArffReader(reader);

        Instances testSet = arff.getData();
        testSet.setClassIndex(data.numAttributes() - 1);

        Configuration conf = new Configuration();

        Job job = Job.getInstance(conf, "KNN");
        job.setJarByClass(KNN.class);

        job.setMapperClass(MapperKNN.class);
        job.setReducerClass(ReducerKNN.class);

        job.setNumReduceTasks(1);

        // job.getConfiguration().setInt(LINES_PER_MAP, 300);

        job.setMapOutputKeyClass(IntWritable.class);
        job.setMapOutputValueClass(PairWritable.class);

        job.setOutputKeyClass(IntWritable.class);
        job.setOutputValueClass(IntWritable.class);

        FileInputFormat.addInputPath(job, new Path(args[0]));
        FileOutputFormat.addOutputPath(job, new Path(args[1]));

        System.exit(job.waitForCompletion(true) ? 0 : 1);

    }
}
