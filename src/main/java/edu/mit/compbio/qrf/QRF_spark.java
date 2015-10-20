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



import java.io.BufferedInputStream;
import java.io.BufferedOutputStream;
import java.io.BufferedReader;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.FileOutputStream;
import java.io.FileReader;
import java.io.IOException;
import java.io.InputStream;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import java.io.OutputStream;
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

import org.apache.commons.io.FileUtils;
import org.apache.commons.io.IOUtils;
import org.apache.log4j.BasicConfigurator;
import org.apache.log4j.Level;
import org.apache.log4j.Logger;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.api.java.function.Function2;
import org.apache.spark.ml.Model;
import org.apache.spark.ml.evaluation.RegressionEvaluator;
import org.apache.spark.ml.param.ParamMap;
import org.apache.spark.ml.regression.RandomForestRegressionModel;
import org.apache.spark.ml.regression.RandomForestRegressor;
import org.apache.spark.ml.tuning.CrossValidator;
import org.apache.spark.ml.tuning.CrossValidatorModel;
import org.apache.spark.ml.tuning.ParamGridBuilder;
import org.apache.spark.mllib.linalg.DenseVector;
import org.apache.spark.mllib.linalg.Vectors;
import org.apache.spark.mllib.regression.LabeledPoint;
import org.apache.spark.sql.DataFrame;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SQLContext;
import org.kohsuke.args4j.Argument;
import org.kohsuke.args4j.CmdLineException;
import org.kohsuke.args4j.CmdLineParser;
import org.kohsuke.args4j.Option;



public class QRF_spark implements Serializable{


	/**
	 * 
	 */
	private static final long serialVersionUID = -8965369734500252128L;

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
	public Integer maxDepth = 5;
	
	@Option(name="-maxBins",usage="maximum number of bins used for splitting features at random forest model, default: 100")
	public Integer maxBins = 100;

	@Option(name="-maxMemoryInMB",usage="maximum memory (Mb) to be used for collecting sufficient statistics at random forest model training. larger usually quicker training, default: 10000")
	public Integer maxMemoryInMB = 10000;

	@Option(name="-featureCols",usage="which column is the class to be identified, allow multiple columns, default: null")
	public ArrayList<Integer> featureCols = null;

	
	@Option(name="-labelCol",usage="which column is the class to be identified, default: 4")
	public int labelCol = 4;



	@Option(name="-sep",usage="seperate character to split each column, default: \\t")
	public String sep = "\\t";

	@Option(name="-h",usage="show option information")
	public boolean help = false;
	
	final private static String USAGE = "QRF_spark [opts] model.qrf inputFile.txt ";

	@Argument
	private List<String> arguments = new ArrayList<String>();

	
	private static Logger log = Logger.getLogger(QRF_spark.class);

	private static long startTime = -1;
	
	private final String featureSubsetStrategy = "auto";
	private final String impurity = "variance";

	//private final String[] inputHeader = new String[]{"log10p", "dist", "hic", "recomb", "recomb_matched","chromStates","label"};
	private final String[] inputHeader = new String[]{"log10p", "hic", "recomb","label"};

	

	/**
	 * @param args
	 * @throws Exception 
	 */
	public static void main(String[] args) throws Exception {
		QRF_spark qrf = new QRF_spark();
		BasicConfigurator.configure();
		Logger.getLogger("org").setLevel(Level.ERROR);
	    Logger.getLogger("akka").setLevel(Level.ERROR);
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
					
					
					
					SparkConf sparkConf = new SparkConf().setAppName("QRF_spark");
					JavaSparkContext sc = new JavaSparkContext(sparkConf);

					
					
					JavaRDD<LabeledPoint> inputData = sc.textFile(inputFile).map(new Function<String, LabeledPoint>(){

						@Override
						public LabeledPoint call(String line) throws Exception {
							String[] tmp = line.split(sep);
							double[] ds = new double[featureCols.size()];
							for(int i = 0; i<featureCols.size(); i++){
								ds[i] = Double.parseDouble(tmp[featureCols.get(i)-1]);
							}
							return new LabeledPoint(Double.parseDouble(tmp[labelCol-1]), Vectors.dense(ds));
						}
						
					});
					
					
					// Prepare training documents, which are labeled.
					SQLContext sqlContext = new SQLContext(sc);

					DataFrame inputDataFrame = sqlContext.createDataFrame(inputData, LabeledPoint.class);
					
					if(train){
						RandomForestRegressor rf = new RandomForestRegressor().setImpurity(impurity)
								.setFeatureSubsetStrategy(featureSubsetStrategy)
								.setNumTrees(numTrees)
								.setMaxBins(maxBins)
								.setMaxDepth(maxDepth)
								.setMaxMemoryInMB(maxMemoryInMB)
								.setSeed(seed);
						
						ParamMap[] paramGrid = new ParamGridBuilder()
						  .build();
						
						//CV
						CrossValidator cv = new CrossValidator()
						  .setEstimator(rf)
						  .setEvaluator(new RegressionEvaluator())
						  .setEstimatorParamMaps(paramGrid)
						  .setNumFolds(kFold); 
						
						// Run cross-validation, and choose the best set of parameters.
						CrossValidatorModel cvModel = cv.fit(inputDataFrame);
						
						double rmse = calculateRMSE(cvModel.bestModel(), inputDataFrame);
						log.info("After cross validation,  RMSE is: " + rmse);
						ObjectOutputStream oos = new ObjectOutputStream(new FileOutputStream(modelFile));
						oos.writeObject(cvModel.bestModel());
						oos.close();
						
					}else{
						if(outputFile == null)
							throw new IllegalArgumentException("Need to provide output file name in -outputFile for Non training mode !!");
						//load trained model
						log.info("Loading model ...");
						ObjectInputStream oosInput = new ObjectInputStream(new FileInputStream(modelFile));
						RandomForestRegressionModel readModel = (RandomForestRegressionModel) oosInput.readObject();
						oosInput.close();
						
						//
						log.info("Read model, predicting ...");
						readModel.transform(inputDataFrame).select("features", "label", "prediction").javaRDD().map(new Function<Row, String>(){

							@Override
							public String call(Row row) throws Exception {
								String tmp = null;
								for(double s : ((DenseVector)row.get(0)).toArray()){
									if(tmp == null){
										tmp = String.valueOf(s);
									}else{
										tmp = tmp + "\t" + String.valueOf(s);
									}
									
								}
								return tmp + "\t" + row.get(1) + "\t" + row.get(2);
							}
							
						}).saveAsTextFile(outputFile + ".tmp");
						log.info("Merging files ...");
						File[] listOfFiles = new File(outputFile + ".tmp").listFiles();
						
						OutputStream output = new BufferedOutputStream(new FileOutputStream(outputFile, true));
			            for (File f : listOfFiles) {
			            	if(f.isFile() && f.getName().startsWith("part-")){
			            		InputStream input = new BufferedInputStream(new FileInputStream(f));
				            	IOUtils.copy(input, output);
				            	IOUtils.closeQuietly(input);
			            	}
			            	
			            }
			            IOUtils.closeQuietly(output);
			            FileUtils.deleteDirectory(new File(outputFile + ".tmp"));
			            
						double rmse = calculateRMSE(readModel, inputDataFrame);
						log.info("For the test dataset,  RMSE is: " + rmse);
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
	
	private double calculateRMSE(Model<?> model, DataFrame test){
		//calculate MSE
		JavaRDD<Row> predictionAndLabel = model.transform(test).select("label", "prediction").javaRDD();
				  
		return Math.sqrt(predictionAndLabel.map(new Function<Row, Double>() {
					@Override
					public Double call(Row pl) throws Exception {
						Double diff = pl.getDouble(0) - pl.getDouble(1);
					    return diff * diff;
						
					}
				  }).reduce(new Function2<Double, Double, Double>() {
					    @Override
					    public Double call(Double a, Double b) {
					      return a + b;
					    }
				  })/predictionAndLabel.count());
	}

}
