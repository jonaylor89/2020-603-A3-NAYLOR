
class KNN {

    public static void setup() {

        /*
         * 
         * create CD reducer matrix of size (size(testSet * k))
         * 
         * Classes can be random but the distances should be set to infinity
         */

    }

    public static void map() {
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
    }

    public static void reduce() {

        /*
         *
         * for i in range(testSet)
         *      cont = 0
         *      for i in range(0, k)
         *          if CDj[i][cont].dist < CD_reducer[i][n].dist
         *              CD_reducer[i][n] = CDj[i][cont]
         *              cont++
         * 
         */

    }

    public static void cleanup() {

        /*
         *
         * for i in range(0, size(testSet))
         *      PredClass[i] = majorityVoting(classes(CD_reducer))
         *      key = i
         *      context.write(key, PredClass[i])
         *
         */

    }

    public static void main(String[] argv) {

    }
}
