/*
 * The MIT License (MIT)
 * Copyright (c) 2015 dnaase <Yaping Liu: lyping1986@gmail.com>

 * Permission is hereby granted, free of charge, to any person obtaining a copy
 * of this software and associated documentation files (the "Software"), to deal
 * in the Software without restriction, including without limitation the rights
 * to use, copy, modify, merge, publish, distribute, sublicense, and/or sell
 * copies of the Software, and to permit persons to whom the Software is
 * furnished to do so, subject to the following conditions:

 * The above copyright notice and this permission notice shall be included in all
 * copies or substantial portions of the Software.

 * THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR
 * IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY,
 * FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL THE
 * AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER
 * LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING FROM,
 * OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN THE
 * SOFTWARE.
 */

/**
 * QRF.java
 * Dec 9, 2014
 * 5:34:04 PM
 * yaping    lyping1986@gmail.com
 */
package main.java.edu.mit.compbio.qrf;



import java.io.BufferedReader;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.FileOutputStream;
import java.io.FileReader;
import java.io.IOException;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import java.io.PrintWriter;
import java.io.Serializable;
import java.nio.file.Files;
import java.nio.file.StandardCopyOption;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Random;

import org.apache.log4j.BasicConfigurator;
import org.apache.log4j.Level;
import org.apache.log4j.Logger;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.api.java.function.Function2;
import org.apache.spark.ml.Model;
import org.apache.spark.ml.classification.RandomForestClassificationModel;
import org.apache.spark.ml.classification.RandomForestClassifier;
import org.apache.spark.ml.evaluation.BinaryClassificationEvaluator;
import org.apache.spark.ml.evaluation.Evaluator;
import org.apache.spark.ml.evaluation.RegressionEvaluator;
import org.apache.spark.ml.param.ParamMap;
import org.apache.spark.ml.regression.RandomForestRegressionModel;
import org.apache.spark.ml.regression.RandomForestRegressor;
import org.apache.spark.ml.tuning.CrossValidator;
import org.apache.spark.ml.tuning.CrossValidatorModel;
import org.apache.spark.ml.tuning.ParamGridBuilder;
import org.apache.spark.mllib.evaluation.BinaryClassificationMetrics;
import org.apache.spark.mllib.linalg.Vectors;
import org.apache.spark.mllib.regression.LabeledPoint;
import org.apache.spark.mllib.tree.RandomForest;
import org.apache.spark.mllib.tree.model.RandomForestModel;
import org.apache.spark.rdd.RDD;
import org.apache.spark.sql.DataFrame;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.RowFactory;
import org.apache.spark.sql.SQLContext;
import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.sql.types.NumericType;
import org.apache.spark.sql.types.StructField;
import org.apache.spark.sql.types.StructType;
import org.kohsuke.args4j.Argument;
import org.kohsuke.args4j.CmdLineException;
import org.kohsuke.args4j.CmdLineParser;
import org.kohsuke.args4j.Option;

import scala.Tuple2;



public class QRF_classifier_spark implements Serializable{



	/**
	 * 
	 */
	private static final long serialVersionUID = -8653251405745184774L;

	/**
	 * @param args
	 */

	@Option(name="-train",usage="only  in the train mode, it will use input file to generate model and saved in model.qrf, default: false")
	public boolean train = false;

//	@Option(name="-testOnly",usage="only  in the test mode when no empty model.qrf is provided, the output will be generated in -outputFile, default: false")
//	public boolean testOnly = false;

	@Option(name="-ignoreCV",usage="ignore the cross validation step, default: false")
	public boolean ignoreCV = false;
	

	@Option(name="-outputFile",usage="the model predicted value on test dataset, default: null")
	public String outputFile = null;

	@Option(name="-seed",usage="seed for the randomization, default: 12345")
	public Integer seed = 12345;
	
	@Option(name="-kFold",usage="number of cross validation during training step, default: 10")
	public int kFold = 10;

	@Option(name="-numTrees",usage="number of tree for random forest model, default: 1000")
	public Integer numTrees = 1000;
	
	@Option(name="-maxDepth",usage="maximum number of tree depth for random forest model, default: 4")
	public Integer maxDepth = 4;
	
	@Option(name="-maxBins",usage="maximum number of bins used for splitting features at random forest model, default: 100")
	public Integer maxBins = 100;

	@Option(name="-featureCols",usage="which column is the class to be identified, allow multiple columns, default: null")
	public ArrayList<Integer> featureCols = null;

	
	@Option(name="-labelCol",usage="which column is the class to be identified, default: 4")
	public int labelCol = 4;



	@Option(name="-sep",usage="seperate character to split each column, default: \\t")
	public String sep = "\\t";

	@Option(name="-h",usage="show option information")
	public boolean help = false;
	
	final private static String USAGE = "QRF_classifier_spark [opts] model.qrf inputFile.txt ";

	@Argument
	private List<String> arguments = new ArrayList<String>();

	
	private static Logger log = Logger.getLogger(QRF_classifier_spark.class);

	private static long startTime = -1;
	
	private final String featureSubsetStrategy = "auto";
	private final String impurity = "entropy";

	//private final String[] inputHeader = new String[]{"log10p", "dist", "hic", "recomb", "recomb_matched","chromStates","label"};
	private final String[] inputHeaders = new String[]{"log10p", "hic", "recomb","label"};

	

	/**
	 * @param args
	 * @throws Exception 
	 */
	public static void main(String[] args) throws Exception {
		QRF_classifier_spark qrf = new QRF_classifier_spark();
		BasicConfigurator.configure();

		qrf.doMain(args);
	}

	public void doMain(String[] args)
			throws Exception {

					CmdLineParser parser = new CmdLineParser(this);
					//parser.setUsageWidth(80);
					try
					{
						if(help || args.length < 2) throw new CmdLineException(USAGE);
						parser.parseArgument(args);
						
					
					}
					catch (CmdLineException e)
					{
						System.err.println(e.getMessage());
						// print the list of available options
						parser.printUsage(System.err);
						System.err.println();
						return;
					}

					//read input bed file, for each row,
					String modelFile = arguments.get(0);
					String inputFile = arguments.get(1);
					initiate();
					
					Logger.getLogger("org").setLevel(Level.WARN);
				    Logger.getLogger("akka").setLevel(Level.WARN);
					
					SparkConf sparkConf = new SparkConf().setAppName("QRF_spark");
					JavaSparkContext sc = new JavaSparkContext(sparkConf);

					

					JavaRDD<Row> inputData = sc.textFile(inputFile).map(new Function<String, Row>(){
						
						@Override
						public Row call(String line) throws Exception {
							String[] tmp = line.split(sep);
							RowFactory rfact = new RowFactory();
							return rfact.create(tmp);
						}
						
					});
					
					
					// Prepare training documents, which are labeled.
					// Generate the schema based on the string of schema
					List<StructField> fields = new ArrayList<StructField>();
					fields.add(DataTypes.createStructField(inputHeaders[0], DataTypes.DoubleType, true));
					fields.add(DataTypes.createStructField(inputHeaders[1], DataTypes.DoubleType, true));
					fields.add(DataTypes.createStructField(inputHeaders[2], DataTypes.DoubleType, true));
					fields.add(DataTypes.createStructField(inputHeaders[3], DataTypes.StringType, false));
					
					StructType schema = DataTypes.createStructType(fields);
					
					SQLContext sqlContext = new SQLContext(sc);

					DataFrame inputDataFrame = sqlContext.createDataFrame(inputData, schema);
							
					if(train){
						RandomForestClassifier rf = new RandomForestClassifier()
								.setFeaturesCol(inputHeaders[0])
								.setFeaturesCol(inputHeaders[1])
								.setFeaturesCol(inputHeaders[2])
								.setLabelCol(inputHeaders[3])
								.setPredictionCol("prediction")
								.setImpurity(impurity)
								.setFeatureSubsetStrategy(featureSubsetStrategy)
								.setNumTrees(numTrees)
								.setMaxBins(maxBins)
								.setMaxDepth(maxDepth)
								.setSeed(seed);

						ParamMap[] paramGrid = new ParamGridBuilder()
						  .build();
						
						BinaryClassificationEvaluator bce = new BinaryClassificationEvaluator()
															.setLabelCol(inputHeaders[3])
															.setRawPredictionCol("prediction");


						
						
						//CV
						//TODO: change back to mllib here, CV with binaryClassfiier does not work for tree based methods....
						CrossValidator cv = new CrossValidator()
						  .setEstimator(rf)
						  .setEvaluator(new RegressionEvaluator())
						  .setEstimatorParamMaps(paramGrid)
						  .setNumFolds(kFold); 
						
						// Run cross-validation, and choose the best set of parameters.
						CrossValidatorModel cvModel = cv.fit(inputDataFrame);
						
						double[] performance = calculateAUC(cvModel.bestModel(), inputDataFrame);
						log.info("After cross validation,  areaUnderROC is: " + performance[0] + "\t areaUnderPR is: " + performance[1]);
						ObjectOutputStream oos = new ObjectOutputStream(new FileOutputStream(modelFile));
						oos.writeObject(cvModel.bestModel());
						oos.close();
						
					}else{
						if(outputFile == null)
							throw new IllegalArgumentException("Need to provide output file name in -outputFile for Non training mode !!");
						//load trained model
						log.info("Loading model ...");
						ObjectInputStream oosInput = new ObjectInputStream(new FileInputStream(modelFile));
						RandomForestClassificationModel readModel = (RandomForestClassificationModel) oosInput.readObject();
						oosInput.close();
						
						//
						log.info("Read model, predicting ...");
						readModel.transform(inputDataFrame).select("features", "label", "prediction").save(outputFile);
						
						double[] performance = calculateAUC(readModel, inputDataFrame);
						log.info("For the test dataset,  areaUnderROC is: " + performance[0] + "\t areaUnderPR is: " + performance[1]);

					}
					
					
					finish();
	}
	
	private void initiate() throws IOException{
		startTime = System.currentTimeMillis();
		if(featureCols == null || featureCols.isEmpty()){
			featureCols = new ArrayList<Integer>();
			featureCols.add(1);featureCols.add(2);featureCols.add(3);
			//throw new IllegalArgumentException("Need to provide featureCols index: " + featureCols.size());
			
		}
			
		
		if(labelCol < 1)
			throw new IllegalArgumentException("labelCol index need to be positive: " + labelCol);

		
	}

	private void finish() throws IOException{
		long endTime   = System.currentTimeMillis();
		double totalTime = endTime - startTime;
		totalTime /= 1000;
		double totalTimeMins = totalTime/60;
		double totalTimeHours = totalTime/3600;
		
		
		log.info("QRF_spark's running time is: " + String.format("%.2f",totalTime) + " secs, " + String.format("%.2f",totalTimeMins) +  " mins, " + String.format("%.2f",totalTimeHours) +  " hours");
	}
	
	private double[] calculateAUC(Model<?> model, DataFrame test){
		//calculate MSE
		JavaRDD<Tuple2<Object, Object>> predictionAndLabel = model.transform(test).select(inputHeaders[3], "prediction").javaRDD().map(new Function<Row, Tuple2<Object, Object>>() {

			@Override
			public Tuple2<Object, Object> call(Row row) throws Exception {
				
				return new Tuple2<Object, Object>(row.get(0), row.get(1));
			}
			
		});
		BinaryClassificationMetrics bcm = new BinaryClassificationMetrics(predictionAndLabel.rdd());
		double[] performances = new double[2];
		performances[0] = bcm.areaUnderROC();
		performances[1] = bcm.areaUnderPR();
		return performances;
	}


}
