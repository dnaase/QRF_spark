/**
 * FdrCorrectionSpark.java
 * Oct 29, 2015
 * 11:06:04 AM
 * yaping    lyping1986@gmail.com
 */
package main.java.edu.mit.compbio.qrf.utils;




import it.unimi.dsi.fastutil.doubles.DoubleBigArrayBigList;
import it.unimi.dsi.fastutil.objects.ObjectBigArrayBigList;

import java.io.File;
import java.io.IOException;
import java.io.PrintWriter;
import java.io.Serializable;
import java.util.ArrayList;
import java.util.List;

import org.apache.log4j.BasicConfigurator;
import org.apache.log4j.Level;
import org.apache.log4j.Logger;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.Function2;
import org.apache.spark.api.java.function.PairFunction;
import org.kohsuke.args4j.Argument;
import org.kohsuke.args4j.CmdLineException;
import org.kohsuke.args4j.CmdLineParser;
import org.kohsuke.args4j.Option;

import scala.Tuple2;



/**
 *
 */
public class FdrCorrectionSpark implements Serializable {

	/**
	 * 
	 */
	private static final long serialVersionUID = 6834860678609273472L;

	@Option(name="-sep",usage="seperate character to split each column, default: \\t")
	public String sep = "\\t";

	@Option(name="-alpha",usage="alpha for control FDR, default: 0.05")
	public double alpha = 0.05;

	@Option(name="-log10p",usage="is the data in -log10 format?, default: false")
	public boolean log10p = false;

	@Option(name="-col",usage="column contain p value, default: 2")
	public int col = 2;

	@Option(name="-indexCols",usage="which columns are the index column, default: 1")
	public ArrayList<Integer> indexCols = null;

	@Option(name="-digits",usage="number of digits after decimal to keep, default: 4")
	public int digits = 4;

	@Option(name="-h",usage="show option information")
	public boolean help = false;

	final private static String USAGE = "FdrCorrectionSpark [opts] pvalue.txt fdr.output.txt";

	@Argument
	private List<String> arguments = new ArrayList<String>();


	private static Logger log = Logger.getLogger(FdrCorrectionSpark.class);
	private static long startTime = -1;



	/**
	 * @param args
	 * @throws Exception 
	 */
	public static void main(String[] args) throws Exception {
		FdrCorrectionSpark fcs = new FdrCorrectionSpark();
		BasicConfigurator.configure();
		Logger.getLogger("org").setLevel(Level.OFF);
	    Logger.getLogger("akka").setLevel(Level.OFF);
	    Logger.getLogger("io.netty").setLevel(Level.OFF);
		fcs.doMain(args);

	}
	
	public void doMain(String[] args)
			throws Exception {

					CmdLineParser parser = new CmdLineParser(this);
					parser.setUsageWidth(80);
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
					String inputFile = args[0];
					String outputFile = args[1];
					
					initiate(outputFile);
					//read and sort data
					SparkConf sparkConf = new SparkConf().setAppName("FdrCorrectionSpark");
					JavaSparkContext sc = new JavaSparkContext(sparkConf);

					
					
					JavaPairRDD<Double, String> inputData = sc.textFile(inputFile).mapToPair(new PairFunction<String, Double, String>(){

						@Override
						public Tuple2<Double, String> call(String row) throws Exception {
							String[] tmp = row.split(sep);
							String index = tmp[indexCols.get(0)-1];
							for(int i = 1; i < indexCols.size();i++){
								index = index + "-" + tmp[indexCols.get(i)-1];
							}
							Double v = Double.parseDouble(tmp[col-1]);
							if(log10p){
								v = Math.pow(10, -v);
							}
							return new Tuple2<Double, String>(v,index);
						}

						
						
					});
					
					final long count = inputData.count();
					
					inputData.mapToPair(new PairFunction<Tuple2<Double, String>, Double, Tuple2<String, Long>>(){

						@Override
						public Tuple2<Double, Tuple2<String, Long>> call(
								Tuple2<Double, String> row) throws Exception {
							
							return new Tuple2<Double, Tuple2<String, Long>>(row._1(),new Tuple2<String,Long>(row._2(), 1L));
						}
						
					}).sortByKey(false).reduceByKey(new Function2<Tuple2<String, Long>, Tuple2<String, Long>, Tuple2<String, Long>>(){

						@Override
						public Tuple2<String, Long> call(
								Tuple2<String, Long> left,
								Tuple2<String, Long> right) throws Exception {
							
							return new Tuple2<String, Long>();
						}
						
					})
					
					inputData.reduce(new Function2<Tuple2<Double, String>, Tuple2<Double, String>, Tuple2<Double, String>>(){

						@Override
						public Tuple3<Double, String> call(
								Tuple2<Double, String> left,
								Tuple2<Double, String> right) throws Exception {
							Double tmp = count*right._1/i;
							
							return null;
						}
						
					});
					
					
					/*
					 * DoubleBigArrayBigList rawPs = new DoubleBigArrayBigList(inputData.keys().toLocalIterator());
					
					ObjectBigArrayBigList<String> keys = new ObjectBigArrayBigList<String>(inputData.values().toLocalIterator());
					
					//sc.close();
					
					Double min = 1.0;
					if(outputFile != null){
						//ObjectOutputStream writer = new ObjectOutputStream(new FileOutputStream(outputFile));
						PrintWriter writer = new PrintWriter(new File(outputFile));
						for (long i = count;i > 0; i--) {
							Double tmp = count*rawPs.getDouble(i-1)/i;
							if(tmp<min){
								min=tmp;
							}
							double p = rawPs.getDouble(i-1);
							if(log10p)
								p = -Math.log10(p);
							writer.println(keys.get(i-1) + "\t" + p + "\t" + min);
						}
						writer.close();
					}
					 */
					
					
					
					finish();
	}
	
	private void initiate(String outputFile) throws IOException{
		startTime = System.currentTimeMillis();
		if(indexCols == null){
			indexCols = new ArrayList<Integer>();
			indexCols.add(1);
		}else if(indexCols.isEmpty()){
			indexCols.add(1);
		}
		
	}

	private void finish() throws IOException{
		
		
		long endTime   = System.currentTimeMillis();
		double totalTime = endTime - startTime;
		totalTime /= 1000;
		double totalTimeMins = totalTime/60;
		double totalTimeHours = totalTime/3600;
		
		log.info("FdrCorrectionSpark's running time is: " + String.format("%.2f",totalTime) + " secs, " + String.format("%.2f",totalTimeMins) +  " mins, " + String.format("%.2f",totalTimeHours) +  " hours");
	}


}
