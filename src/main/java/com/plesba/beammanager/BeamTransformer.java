package com.plesba.beammanager;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

import java.io.IOException;
import java.util.List;

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
    public BeamTransformer(String parameterInputFile, String parameterOutputFile ) {

        LOG.info("BeamTransformer started processing with file parameters: " + parameterInputFile + "/" + parameterOutputFile);
        csvInFile = parameterInputFile;
        csvOutFile = parameterOutputFile;
    }

    public void processDataFromInput() throws IOException {

        try {
            LOG.info("BeamTransformer reading from input file/writing to output file");
            //using Direct Runner as default
            PipelineOptions options = PipelineOptionsFactory.create();
            Pipeline p = Pipeline.create(options);


            //create input collection
            PCollection<String> lines = p.apply("ReadMyFile", TextIO.read().from(csvInFile));
            PCollection<String> outlines = lines.apply();
            p.run();

            LOG.info("BeamTransformer transformation performed; output collection created ");

            LOG.info(String.format("BeamTransformer finished processing");

        } catch (IOException e) {
            e.printStackTrace();
        }
    }


}
