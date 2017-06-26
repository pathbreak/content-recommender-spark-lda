package com.pathbreak.lda

import scala.collection.mutable.WrappedArray

import org.apache.spark.{SparkContext}
import org.apache.spark.ml.Pipeline
import org.apache.spark.ml.feature.{CountVectorizer, CountVectorizerModel, Tokenizer, StopWordsRemover, BucketedRandomProjectionLSH}
import org.apache.spark.ml.linalg.{Vector => MLVector}
import org.apache.spark.ml.clustering.LDA
import org.apache.spark.mllib.linalg.{Vector, Vectors}
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.{Row, SparkSession}

object Lda {
    def main(args: Array[String]) {
        
        val trainingDirectory = args(0)
        val testingDirectory = args(1)
        val numTopics = args(2).toInt
        val iterations = args(3).toInt

        // Making EM (Expectation Maximization) as default algorithm 
        // because it's much more stable, less resource intensive and far faster
        // than online (variational Bayes) implementation.
        val algo = if (args.length > 4) args(4) else "em" // "em" | "online"
        
        val customStopsFile = if (args.length > 5) args(5) else null
        val fileFormat = if (args.length > 6) args(6) else "json"
        
        val spark = SparkSession.builder().appName("LDA").getOrCreate()
        
        import spark.implicits._
        
        val sc = spark.sparkContext
        
        val t0 = System.nanoTime()

        
        val rawTrain = 
            if (fileFormat == "json")
                spark.read.json(trainingDirectory)
            else
                sc.wholeTextFiles(trainingDirectory).toDF("id", "contents")
        rawTrain.cache()
        
        // Tokenizer
        val tokenizer = new Tokenizer().setInputCol("contents").setOutputCol("rawTokens")
        
        // Stop words remover
        val stopsRemover = new StopWordsRemover().setInputCol("rawTokens").setOutputCol("tokens")
        val customStops = 
            if (customStopsFile != null) 
                sc.textFile(customStopsFile).collect() 
            else 
                Array[String]()
        stopsRemover.setStopWords(stopsRemover.getStopWords  union  customStops union Array(" ", ""))
        
        // Term counts vectorizer
        val cvec = new CountVectorizer().setInputCol("tokens").setOutputCol("counts")
        
        
        val pipeline = new Pipeline().setStages( Array(tokenizer, stopsRemover, cvec) )
        
        // Get term count matrix
        val model = pipeline.fit(rawTrain)
        
        /* Term counts RDD for use with o.a.s.mll.clustering:*/
        val termCounts = model.transform(rawTrain)
        termCounts.cache()
        
        val vocabArray = model.stages(2).asInstanceOf[CountVectorizerModel].vocabulary
        
        // Run LDA. 
        // Input is the "counts" column of DF passed to fit.
        // The topic distribution for each document is output in "topics" column
        val lda = new LDA()
            .setOptimizer(algo)
            .setFeaturesCol("counts")
            .setTopicDistributionCol("topics")
            .setK(numTopics)
            .setMaxIter(iterations)
        
        val ldaModel = lda.fit(termCounts)
        //println(s"\n\n\nLDA Model: $ldaModel")
        val trainSetTopics = ldaModel.transform(termCounts) //.limit(5))
        trainSetTopics.cache()
        //println(s"""LDAModel Transform output: ${trainSetTopics.columns.mkString(",")}""")
        
        //println(trainSetTopics.select("topics").show(1, false))
        
        println("\n\n\n")
        
        // Print topics. The DF returned by describeTopics() contains 3 columns:
        //  - "topic": IntegerType: topic index
        //  - "termIndices": ArrayType(IntegerType): term indices, sorted in order of decreasing term importance
        //  - "termWeights": ArrayType(DoubleType): corresponding sorted term weights
        //
        val topicWeights = ldaModel.describeTopics(maxTermsPerTopic = 10)
        //println(s"Topic weights:$topicWeights")
        //topicWeights.collect().foreach { println(_) }
        
        val testset = 
            if (fileFormat == "json")
                spark.read.json(testingDirectory)
            else
                sc.wholeTextFiles(testingDirectory).toDF("id", "contents")
        
        testset.cache()
        val testsetTermCounts = model.transform(testset)
        testsetTermCounts.cache()
        val testsetTopics = ldaModel.transform(testsetTermCounts)
        testsetTopics.cache()
        
        //println(testsetTopics.select("topics").show(1, false))
        
        val lsh = new BucketedRandomProjectionLSH()
                        .setBucketLength(10)
                        .setNumHashTables(5)
                        .setInputCol("topics")
                        .setOutputCol("values")
                        
        val lshModel = lsh.fit(trainSetTopics)
        
        // The returned Dataframe has 3 columns: 
        // - "datasetA" is same as first arg
        // - "datasetB" is same as second arg
        // - "distCol" is the distance between the rows
        var similar = lshModel.approxSimilarityJoin(trainSetTopics, testsetTopics, 0.1, "distance")
        //val similarNumRows = similar.count
        
        similar = similar.dropDuplicates("datasetB").orderBy("distance")
        //val similarNumRowsWithoutDups = similar.count
        
        similar = similar.limit(20)
            
        //println(s"\n\nSimilarity join dataset: ${similar.columns.mkString(",")}\n\n")
        println(s"\n\nSimilarity join dataset:\n\n")
        
        //similar.foreach( printRow(_) )
        /*
        val firstRow:Row = similar.first().asInstanceOf[Row]
        println("First Row size: " + firstRow.size)
        for (i <- 0 to firstRow.size-1) {
            println(s"First Row field $i: " + firstRow.get(i).getClass)
        }
        println(firstRow.getAs[Row]("datasetA").getAs[String]("id"))
        */
        similar.take(20).foreach( r => {
            val x = r.asInstanceOf[Row]
            
            val historyRow = x.getAs[Row]("datasetA")
            val historyItemTitle = historyRow.getAs[String]("title")
            val historyItemUrl = historyRow.getAs[String]("url")
            val historyItemTopicDist = historyRow.get(historyRow.fieldIndex("topics"))
            
            val targetRow = x.getAs[Row]("datasetB")
            val targetItemTitle = targetRow.getAs[String]("title")
            val targetItemUrl = targetRow.getAs[String]("url")
            val targetItemTopicDist = targetRow.get(targetRow.fieldIndex("topics"))
            
            println(s"\nRecommendation:\n\t$targetItemTitle\n\t$targetItemUrl\n")
            println(s"\tTopics: $targetItemTopicDist\n")
            println(s"  based on:\n\t$historyItemTitle\n\t$historyItemUrl\n")
            println(s"\tTopics: $historyItemTopicDist\n")
        } )
        
        println("Topics:")
        topicWeights.collect.foreach  { r => 
            val x = r.asInstanceOf[Row]
            println(s"\n\tTopic ${x.getInt(0)}:")
            val termIndices = x.getAs[WrappedArray[Int]]("termIndices")
            val termWeights = x.getAs[WrappedArray[Double]]("termWeights")
            val termInfo = termIndices zip termWeights
            termInfo.foreach { case (termIndex, termWeight) =>
                println(s"\t\t${vocabArray(termIndex)} : $termWeight")
            }
        }
        
        spark.stop()
        
        val t1 = System.nanoTime()
        
        //println(s"\n\nSimilarity join dataset rows with dupes: $similarNumRows\n\n")
        //println(s"\n\nSimilarity join dataset rows without dupes: $similarNumRowsWithoutDups\n\n")
        println(s"Time taken for LDA:${(t1-t0) / (1e9)} s")
    }
}
