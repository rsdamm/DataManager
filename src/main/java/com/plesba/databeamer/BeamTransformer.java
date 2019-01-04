package com.plesba.databeamer;

import org.apache.beam.sdk.transforms.DoFn;
import org.apache.beam.sdk.transforms.ParDo;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

import java.io.IOException;
import java.util.List;
import java.util.Properties;

import org.apache.beam.sdk.Pipeline;
import org.apache.beam.sdk.io.TextIO;
import org.apache.beam.sdk.options.PipelineOptions;
import org.apache.beam.sdk.options.PipelineOptionsFactory;
import org.apache.beam.sdk.values.PCollection;

public class BeamTransformer {
    private PCollection inputCollection;
    private PCollection outputCollection;
    private int recordCount = 0;
    private int recordCountO = 0;
    private byte[] theByteArray = null;
    private static final Log LOG = LogFactory.getLog(BeamTransformer.class);
    private String csvInFile;
    private String csvOutFile;
    
    private List<String> stringList = null;

    //  input collection; creates a Beam pipeline, transforms; writes to output collection
    public BeamTransformer(Properties parameterProperties ) {

        String inFileOverride = parameterProperties.getProperty("beam.infilename");
        if (inFileOverride != null) {
            csvInFile = inFileOverride;
        }

        LOG.info("BeamTransformer input filename "+ csvInFile);

        String outFileOverride = parameterProperties.getProperty("beam.outfilename");
        if (outFileOverride != null) {
            csvOutFile = outFileOverride;
        }

        LOG.info("BeamTransformer out filename "+ csvOutFile);

        LOG.info("BeamTransformer started processing with files: " + csvInFile + "/" + csvOutFile);
    }
    static class ComputeWordLengthFn extends DoFn<String, Integer> {

    }
    public void processDataFromInput() throws IOException {

        try {
                LOG.info("BeamTransformer reading from input file/writing to output file");
                //using Direct Runner as default
                PipelineOptions options = PipelineOptionsFactory.create();
                Pipeline p = Pipeline.create(options);

                //create input collection
                PCollection<String> words = p.apply(TextIO.read().from(csvInFile));

                //apply Pardo
                PCollection<Integer> wordLengths = words.apply("ComputeWordLengths",
                    ParDo.of(new ComputeWordLengthFn()));

                p.run();

                LOG.info("BeamTransformer transformation performed; output collection created ");

                LOG.info("BeamTransformer finished processing");

        } catch (Exception e) {
            e.printStackTrace();
        }
    }


}
