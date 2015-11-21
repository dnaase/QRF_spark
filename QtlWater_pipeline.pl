 # The MIT License (MIT)
 # Copyright (c) 2015 dnaase <Yaping Liu: lyping1986@gmail.com>

 # Permission is hereby granted, free of charge, to any person obtaining a copy
 # of this software and associated documentation files (the "Software"), to deal
 # in the Software without restriction, including without limitation the rights
 # to use, copy, modify, merge, publish, distribute, sublicense, and/or sell
 # copies of the Software, and to permit persons to whom the Software is
 # furnished to do so, subject to the following conditions:

 # The above copyright notice and this permission notice shall be included in all
 # copies or substantial portions of the Software.

 # THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR
 # IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY,
 # FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL THE
 # AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER
 # LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING FROM,
 # OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN THE
 # SOFTWARE.
 
#!/usr/bin/perl -w
##QtlWater_pipeline.pl: the main entry of QtlWater software


## author: Yaping Liu  lyping1986@gmail.com 
## time: 2015-7-21

#Usage: perl QtlWater_pipeline.pl [option] configure.txt

use strict;
use Getopt::Long;
use File::Basename;


sub usage {

    print STDERR "\nUsage:\n";
    print STDERR "perl QtlWater_pipeline.pl [option] prefix configure.txt\n\n";
    print STDERR "Require R-3.0 (MatrixEQTL library) or above, Java-1.7 or above and bigWigAverageOverBed (download from UCSC utils) to installed\n\n";

    print STDERR " [Options]:\n\n";
    print STDERR " ###############General options:\n\n";
	print STDERR "  --mode NUM : 1. Detect meQTL/eQTL;(Default mode)\n";
	print STDERR "  			 2. Generate training file;(TODO)\n";
	print STDERR "  --region STR : Specify the region for mode 1 or 2. Should be format like: chr1:1-1000 or chr1 (Default: chr1).\n";
	print STDERR "  --fdr NUM : Specify the false discovery rate for Matrix EQTL and QRF (Default: 0.01).\n";
	print STDERR "  --mem NUM : Specify the number of gigabytes in the memory to use (Default: 15).\n";
	print STDERR "  --cpu NUM : Specify the number of CPU to use (Default: 1).\n";
	print STDERR "  --qtlwater_path DIR : Specify the QTLWATER root direcotry (Default: not specified. use environment variable \$QTLWATER).\n\n";

    print STDERR " ###############Options for mode 1:\n\n";
	print STDERR "  --tree_num NUM : Specify the number of tree used for QRF (Default: 1000).\n";
	print STDERR "  --sep STR : Specify the string used to seperate the column (Default: \\\\t, tab delimit).\n";
	print STDERR "  --hic_resolution NUM : Specify the resolution of HiC signal used, default is 1kb (Default: 1).\n\n";

    print STDERR " ###############Options for mode 2:\n\n";
	print STDERR "  --ground_truth_data STR : Specify the ground truth datset for QRF training (Default: null).\n";
	print STDERR "  --ground_truth_data_indexs NUM : Specify the column in the ground truth datset for identify eQTL/meQTL pairs (Default: null).\n";
	print STDERR "  --ground_truth_data_rawp NUM :  Specify the raw p value column in the ground truth datset (Default: null).\n";
	print STDERR "  --ground_truth_data_fdr NUM :  Specify the FDR value column in the ground truth datset (Default: null).\n";
	print STDERR "  --positive_probes_sampling NUM : Specify the number of positive probes used for QRF training (Default: 10000).\n";
	print STDERR "  --negative_probes_sampling NUM : Specify the number of negative probes used for QRF training (Default: 10000).\n\n";

    print STDERR " ###############Options for mode 3:\n\n";
	print STDERR "  --r2 NUM : Specify the r square threshold for LD independent interval generation (Default: 0.8).\n";
	print STDERR "  --ld_dist NUM : Specify the distance threshold for LD independent interval generation (Default: 250000).\n\n";

			
    exit(1);
}


##default option setting
my $mode=1;

my $mem=15;
my $cpu=1;
my $region="chr1";
my $fdr=0.01;
my $qtlwater_path=`echo \$QTLWATER`;
chomp($qtlwater_path);

my $tree_num=1000;
my $sep="\\\\t";
my $hic_resolution=1;

my $ground_truth_data="";
my @ground_truth_data_indexs=();
my $ground_truth_data_rawp="";
my $ground_truth_data_fdr="";
my $positive_probes_sampling=10000;
my $negative_probes_sampling=10000;

my $omit_matrixeqtl="";
my $omit_matrixeqtl_result_anno="";
my $omit_add_hic="";
my $omit_add_recomb="";
my $omit_make_input_matrix="";
my $omit_qtlwater="";
my $omit_qtlwater_result_anno="";
my $omit_fdr_correction="";

GetOptions( 
			"mode=i" => \$mode,
			"mem=i" => \$mem,
			"cpu=i" => \$cpu,
			"region=s" => \$region,
			"fdr=f" => \$fdr,
			"qtlwater_path=s" => \$qtlwater_path,
			"tree_num=i" => \$tree_num,
			"sep=s" => \$sep,
			"hic_resolution=i" => \$hic_resolution,
			"ground_truth_data=s" => \$ground_truth_data,
			"ground_truth_data_indexs=i" => \@ground_truth_data_indexs,
			"ground_truth_data_rawp=i" => \$ground_truth_data_rawp,
			"ground_truth_data_fdr=i" => \$ground_truth_data_fdr,
			"positive_probes_sampling=i" => \$positive_probes_sampling,
			"negative_probes_sampling=i" => \$negative_probes_sampling,

			"omit_matrixeqtl" => \$omit_matrixeqtl,
			"omit_matrixeqtl_result_anno" => \$omit_matrixeqtl_result_anno,
			"omit_add_hic" => \$omit_add_hic,
			"omit_add_recomb" => \$omit_add_recomb,
			"omit_make_input_matrix" => \$omit_make_input_matrix,
			"omit_qtlwater" => \$omit_qtlwater,
			"omit_qtlwater_result_anno" => \$omit_qtlwater_result_anno,
			"omit_fdr_correction" => \$omit_fdr_correction,
			
);



check_parameter();

my $prefix=$ARGV[0];
my $conf_file=$ARGV[1];

my ($dir,$matrixqtlresult,$snp_loc,$gene_loc,$snp_tab,$gene_tab,$cov_tab,$recomb, $hic, $train) = check_conf_file($conf_file);

my $chr=$region;
if($chr=~/\b\w+:\S+/){
		$chr=~s/:\S+//;
		
}

##call meQTL/eQTL by MatrixEQTL
if(($omit_matrixeqtl eq "") and (($matrixqtlresult eq "") or ($matrixqtlresult eq "NULL"))){

	my $tmp=`wc $dir/$snp_tab`;
	chomp($tmp);
	my @f=split " ",$tmp;
	my $sampleSize=int($f[1]/$f[0])-1;
	print STDERR "no Matrix EQTL results provided. So called MatrixEQTL now ...\n\n";
	my $cmd="R --no-save --no-restore --args qrf_path=$qtlwater_path wd=$dir snpInfo=$snp_tab exprInfo=$gene_tab  snpLoc=$snp_loc exprLoc=$gene_loc covarInfo=$cov_tab prefix=$prefix chr=$chr sampleSize=$sampleSize < $qtlwater_path/src/main/R/call_meqtl_random.byChr.R\n";
	run_cmd($cmd);
	$matrixqtlresult = "$dir/cis-qtl.matrixEQtlAll.SampleSize-$sampleSize.$prefix.$chr.txt";
}else{
	$matrixqtlresult="$dir/$matrixqtlresult";
}

##annotate MatrixEQTL result with coordinate
my $matrixqtlresult_cor=$matrixqtlresult;
$matrixqtlresult_cor=~s/\.\w+$/.sig.sameChr.addCor.bed/;
my $matrixqtlresult_cor_uniq=$matrixqtlresult_cor;
$matrixqtlresult_cor_uniq=~s/\.\w+$/.uniq.bed/;
my $cmd = "perl $qtlwater_path/src/main/perl/add_cor_to_matrixqtl.pl $matrixqtlresult $dir/$snp_loc $dir/$gene_loc\n";
if($omit_matrixeqtl_result_anno eq ""){
	print STDERR "Adding genomic coordinate to MatrixEQTL result ...\n\n";
	run_cmd($cmd);
	#get uniq bed file
	$cmd = "perl $qtlwater_path/src/main/perl/uniqLine.pl $matrixqtlresult_cor $matrixqtlresult_cor_uniq\n";
	run_cmd($cmd);
	
}



##attach HiC signal
my $matrixqtlresult_hic=$matrixqtlresult_cor_uniq;
$matrixqtlresult_hic=~s/\.\w+$/.hic_signal.bed/;
if($omit_add_hic eq ""){
	print STDERR "Adding HiC signal to MatrixEQTL result ...\n\n";
	$cmd = "perl $qtlwater_path/src/main/perl/get_hic_freq.sparse.pl $hic_resolution $matrixqtlresult_cor_uniq $dir/$hic 1 1 > $matrixqtlresult_hic\n";
	run_cmd($cmd);
	
}

##attach recombination rate

my $matrixqtlresult_recomb_prefix=$matrixqtlresult_hic;
$matrixqtlresult_recomb_prefix=~s/\.\w+$/.recomb_signal/;
if($omit_add_recomb eq ""){
	print STDERR "Adding recombination rate signal to MatrixEQTL result ...\n\n";
	$cmd = "perl $qtlwater_path/src/main/perl/alignWig2Bed.bigWigAverageOverBed.pl $matrixqtlresult_recomb_prefix $matrixqtlresult_hic $dir/$recomb --min_data 1 \n";
	run_cmd($cmd);
	
}
my $recomb_prefix=basename($recomb);
$recomb_prefix=~s/(\w+)\S+/$1/;
my $matrixqtlresult_recomb=$matrixqtlresult_recomb_prefix.".alignTo.$recomb_prefix.min_data-1.txt";


if($mode == 1){
	##make matrix file for QTLWATER decoding
	my $input_matrix=$matrixqtlresult_cor_uniq;
	$input_matrix=~s/\.\w+$/.log10p_hic_recomb.txt/;
	if($omit_make_input_matrix eq ""){
		$cmd=" cut -f4-5,9-11 $matrixqtlresult_recomb | perl -ne 'chomp;\@f=split \"\\t\";\$name=\"\$f[0]-\$f[1]\";\$f[2]=-log(\$f[2])/log(10);if(\$f[4] ne \"NA\"){print \"\$name\\t\$f[2]\\t\$f[3]\\t\$f[4]\\n\";}' > $input_matrix\n";
		run_cmd($cmd);
	
	}
		##run QTLWATER model
	my $output_matrix=$input_matrix;
	$output_matrix=~s/\.\w+$/.afterQtlWater.txt/;
	if($omit_qtlwater eq ""){
		print STDERR "QTLWATER core model ...\n\n";
		$cmd = "spark-submit --jars $qtlwater_path/lib/args4j-2.0.31.jar,$qtlwater_path/lib/sparkling-water-assembly-1.5.3-all.jar,$qtlwater_path/lib/fastutil-7.0.8.jar --driver-memory ${mem}G --class main.java.edu.mit.compbio.qrf.QtlSparklingWater --master local[$cpu] $qtlwater_path/target/QtlWater-0.5.jar -outputFile $output_matrix $dir/$train $input_matrix -numTrees $tree_num -indexCols 1 -featureCols 2 -featureCols 3 -featureCols 4 -labelCol 5\n";
		run_cmd($cmd);
	}


	##FDR correction
	my $output_matrix_fdr=$output_matrix;
	$output_matrix_fdr=~s/\.\w+$/.fdr.txt/;
	if($omit_fdr_correction eq ""){
		print STDERR "BH FDR correction ...\n\n";
		$cmd = "spark-submit --jars $qtlwater_path/lib/args4j-2.0.31.jar,$qtlwater_path/lib/sparkling-water-assembly-1.5.3-all.jar,$qtlwater_path/lib/fastutil-7.0.8.jar --driver-memory ${mem}G --class main.java.edu.mit.compbio.qrf.utils.FdrCorrectionSpark --master local[$cpu] $qtlwater_path/target/QtlWater-0.5.jar $output_matrix $output_matrix_fdr -indexCols 1 -col 5 -log10p\n";
		run_cmd($cmd);
	}
	
	if($omit_make_input_matrix eq ""){
		`unlink $input_matrix`;
	}


	if($omit_qtlwater eq ""){
		`unlink $output_matrix`;
	}
	
}if($mode == 2){
	##make matrix file for QTLWATER training
	my $input_matrix=$matrixqtlresult_cor_uniq;
	$input_matrix=~s/\.\w+$/.log10p_hic_recomb.training_pos_${positive_probes_sampling}_neg_${negative_probes_sampling}.txt/;
	if($omit_make_input_matrix eq ""){
		my $pos_num=0;
	my $neg_num=0;
	my %hash_rawp=();
	my %hash_fdr=();
	$ground_truth_data_rawp--;
	$ground_truth_data_fdr--;
	open(ANNO,"<$ground_truth_data") or die;
	while(<ANNO>){
		my @f=split "\t";
		if($f[0] eq "SNP"){
			next;
		}
		my $t=$ground_truth_data_indexs[0]-1;
		my $key="$f[$t]";
		for(my $z=1;$z<=$#ground_truth_data_indexs;$z++){
			my $ti=$ground_truth_data_indexs[$z]-1;
			$key="${key}-$f[$ti]";
		}
		$hash_rawp{$key} = -log($f[$ground_truth_data_rawp])/log(10);
		$hash_fdr{$key} = $f[$ground_truth_data_fdr];
		if($f[$ground_truth_data_fdr] < $fdr){
			$pos_num++;
		}else{
			$neg_num++;
		}
	}
	
	
	open(F,"<$matrixqtlresult_recomb") or die;
	open(OUT,">$input_matrix") or die;
	while(my $d=<F>){
		chomp($d);
		my @f=split "\t",$d;
		if($f[10] eq "NA"){
			next;
		}
		my $key="$f[3]-$f[4]";
		if(exists $hash_rawp{$key}){
			my $r=rand(1);
			if($hash_fdr{$key} < $fdr){
				if($r<$positive_probes_sampling/$pos_num){
					my $logp=-log($f[8])/log(10);
					print OUT "$key\t$logp\t$f[9]\t$f[10]\t$hash_rawp{$key}\n";
				}
			}else{
				if($r<$negative_probes_sampling/$neg_num){
					my $logp=-log($f[8])/log(10);
					print OUT "$key\t$logp\t$f[9]\t$f[10]\t$hash_rawp{$key}\n";
				}
			}
			
		}
	}
	close(F);
	close(OUT);
	}
	
	
	##use QTLWATER training for training

	if($omit_qtlwater eq ""){
		print STDERR "training in QTLWATER core model ...\n\n";
		$cmd = "spark-submit --jars $qtlwater_path/lib/args4j-2.0.31.jar,$qtlwater_path/lib/sparkling-water-assembly-1.5.3-all.jar,$qtlwater_path/lib/fastutil-7.0.8.jar --driver-memory ${mem}G --class main.java.edu.mit.compbio.qrf.QtlSparklingWater --master local[$cpu] $qtlwater_path/target/QtlWater-0.5.jar -train $dir/$train $input_matrix -numTrees $tree_num -indexCols 1 -featureCols 2 -featureCols 3 -featureCols 4 -labelCol 5\n";
		run_cmd($cmd);
	}
	
	if($omit_make_input_matrix eq ""){
		`unlink $input_matrix`;
	}

	

}

	
	


##delete temprary files
if(($omit_matrixeqtl eq "") and (($matrixqtlresult eq "") or ($matrixqtlresult eq "NULL"))){
		my $tmp="$matrixqtlresult";
		$tmp=~s/cis-qtl/trans-qtl/;
		`unlink $tmp`;
}

if($omit_matrixeqtl_result_anno eq ""){
	`unlink $matrixqtlresult_cor`;
	`unlink $matrixqtlresult_cor_uniq`;
	$matrixqtlresult_cor=~s/\.bed$//;
	`unlink ${matrixqtlresult_cor}.Cpg.bed`;
	`unlink ${matrixqtlresult_cor}.Snp.bed`;
}


if($omit_add_hic eq ""){
	`unlink $matrixqtlresult_hic`;
}


if($omit_add_recomb eq ""){
	`unlink $matrixqtlresult_recomb`;
}





##compare it with MatrixEQTL

##use plink to generate LD independent interval, and find GWAS category enrichment


##########################################All Subroutines###############################################################
sub check_parameter{
	usage() if ( scalar(@ARGV) <= 1 );
	
	if($mode != 1 && $mode != 2){
		print STDERR "Wrong mode number!!\n\n";
		usage();
	}
	
}


sub check_conf_file{
	my $input=shift @_;
	my $dir="";
	my $matrixqtlresult="";
	my $snp_loc="";
	my $gene_loc="";
	my $snp_tab="";
	my $gene_tab="";
	my $cov_tab="";
	my $recomb="";
	my $hic="";
	my $train="";
	
	open(FH,"<$input") or die "can't open configure file $input:$!\n";
	while(<FH>){
		chomp;
		my @f=split "=";
		if($f[0] eq "directory"){
			$dir=$f[1];
		}elsif($f[0] eq "MatrixQtlResult"){
			$matrixqtlresult=$f[1];
		}elsif($f[0] eq "SNP_loc"){
			$snp_loc=$f[1];
		}elsif($f[0] eq "gene_loc"){
			$gene_loc=$f[1];
		}elsif($f[0] eq "SNP_tab"){
			$snp_tab=$f[1];
		}elsif($f[0] eq "gene_tab"){
			$gene_tab=$f[1];
		}elsif($f[0] eq "covar_tab"){
			$cov_tab=$f[1];
		}elsif($f[0] eq "recombination_bw"){
			$recomb=$f[1];
		}elsif($f[0] eq "HiC_KRnorm"){
			$hic=$f[1];
		}elsif($f[0] eq "training_file"){
			$train=$f[1];
		}
	}
	close(FH);
	
	##check parameter read
	if(($dir eq "") or ($dir eq "NULL")){
		$dir="./";
	}
	if(($recomb eq "") or ($recomb eq "NULL")){
		die "recombination rate bigwig file could not be NULL!!\n\n";
	}
	if(($hic eq "") or ($hic eq "NULL")){
		die "HiC file could not be NULL!!\n\n";
	}
	if(($train eq "") or ($train eq "NULL")){
		die "Training file name could not be NULL!!\n\n";
		if(($mode == 1) and ((!-e $train) or (-z $train))){
			die "Training file does not exists or is emptyL!!\n\n";
		}
	}
	if(($matrixqtlresult ne "") and ($matrixqtlresult ne "NULL") and (-e $matrixqtlresult) or (! -z $matrixqtlresult)){
		if(($snp_loc eq "") or ($snp_loc eq "NULL") or ($gene_loc eq "") or ($gene_loc eq "NULL")){
			die "SNP location and Gene/CpG location could not be NULL!!\n\n";
		}
	}else{
		if(($snp_loc eq "") or ($snp_loc eq "NULL") or ($gene_loc eq "") or ($gene_loc eq "NULL") or ($snp_tab eq "") or ($snp_tab eq "NULL") or ($gene_tab eq "") or ($gene_tab eq "NULL")){
			die "SNP location, Gene/CpG location, SNP information and Gene/CpG information could not be NULL!!\n\n";
		}
		if((! -e $matrixqtlresult) or (-z $matrixqtlresult)){
			die "MatrixEQTL result file $matrixqtlresult can not be found or is empty!!\n\n";
		}
	}
	
	return($dir,$matrixqtlresult,$snp_loc,$gene_loc,$snp_tab,$gene_tab,$cov_tab,$recomb, $hic, $train);
}



sub run_cmd{
	my $cmd=shift @_;
	print STDERR "$cmd\n";
	system($cmd) == 0 || die "can't execute command $cmd:$!\n";
}
