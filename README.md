# QtlWater
A meQTL/eQTL detection method based on Gradient Boost Tree model by using recombination rate and HiC signal to boost the power.

Liu Y & Kellis M. QtlWater: boosting molecular quantitative trait loci mapping power by incorporating recombination rate and chromatin conformation changes. In preparation.

## Table of Contents
1. [Quick start](#quick-start)
2. [Installation](#installation)
3. [Usage](#usage)

## Quick start
1. Install
	```
	git clone --recursive https://github.com/dnaase/QtlWater.git
	cd QtlWater
	mvn clean package
	```

2. Run the [example test file](configure.txt)
	```
	unzip test_data_zip.zip
	mv test_data_zip test_data
	perl QtlWater_pipeline.pl test configure.txt
	```
	
	This should produce the following files:
	* cis-qtl.matrixEQtlAll.SampleSize-133.test.chr1.txt (MatrixEQTL result)
	* cis-qtl.matrixEQtlAll.SampleSize-133.test.chr1.sig.sameChr.addCor.uniq.log10p_hic_recomb.afterQtlWater.fdr.txt (QtlWater FDR result)

## Installation
#### Prerequisites
* Java 7 (Oracle)
* R-3.0 or above (https://www.r-project.org/)
* Perl 5
* MatrixEQTL package in R (http://www.bios.unc.edu/research/genomic_software/Matrix_eQTL/runit.html or https://cran.r-project.org/web/packages/MatrixEQTL/index.html)
* bigWigAverageOverBed from UCSC utils (http://hgdownload.soe.ucsc.edu/admin/exe/)

Compilation requires mvn 3.0 or above

	```
	git clone --recursive https://github.com/dnaase/QtlWater.git
	cd QtlWater
	mvn clean package
	```

#### required files
1. recombination rate big wig file, which could be download from 1000 Genome ftp fite
2. Hi-C signal file, which could be download from (Rao et al. 2015 Cell)

## Usage

### Training

#### Examples
```
perl QtlWater_pipeline.pl test configure.txt  --mode 2 --ground_truth_data test_data/cis-qtl.matrixEQtlAll.SampleSize-133.test.chr1.txt --ground_truth_data_indexs 1 --ground_truth_data_indexs 2 --ground_truth_data_rawp 5 --ground_truth_data_fdr 6
```


### Compute enhanced p value and FDR (BH)

#### Examples
```
perl QtlWater_pipeline.pl test configure.txt
```












	
