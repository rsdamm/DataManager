
package com.plesba.datamanager.source;

import com.opencsv.CSVReader;
import java.io.FileReader;
import java.io.IOException;
import java.io.PipedOutputStream;

import java.util.logging.Level;
import java.util.logging.Logger;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

// reads a csv file and writes to output stream
public final class CSVSource
{    
    private final String fileToRead;
    
    private com.opencsv.CSVReader reader = null;
    private PipedOutputStream outputStream = null;
    private int recordCount = 0; 
    private String [] nextLine = null;  
    private byte[] theByteArray = null;
    private StringBuilder recordStringBuffer;
    private int i=0;

    private static final Log LOG = LogFactory.getLog(CSVSource.class);
    
    public CSVSource(String rfn, PipedOutputStream parameterOutputStream ) {
       fileToRead = rfn;  
       outputStream = parameterOutputStream;
       csvreaderSetup();
    
    }
    public CSVSource() {
        this.recordStringBuffer = null;
            fileToRead = null; 
            outputStream = null;
            reader = null; 
    }
 
    public void csvreaderSetup() {

        LOG.info("CSVSource started processing.");
        try {
             //Get the CSVSource instance specifying the delimiter to be used
            reader = new com.opencsv.CSVReader(new FileReader(fileToRead),',');
            }   
        catch (IOException e) {
            System.err.println (e);
        }
 
    }
   public void putDataOnOutputStream () throws RuntimeException {


       recordStringBuffer = new StringBuilder();

       try {
       while ((nextLine = reader.readNext()) != null)
        {
          for (i=0; i < nextLine.length; i++) {

            if (i>0) {
                recordStringBuffer.append(",");
            }

            recordStringBuffer.append(nextLine[i]);

          }
          recordStringBuffer.append("\n");

          theByteArray = recordStringBuffer.toString().getBytes();
          outputStream.write(theByteArray);

          LOG.info("CSVSource writing record to stream---> "+ recordStringBuffer);
          recordCount++;
          recordStringBuffer.setLength(0);
        }

        LOG.info("CSVSource finished processing.");
        outputStream.close();

       } catch (IOException e) {
                    throw new RuntimeException(e);
       }
   }
   public int getReadCount ()    {
        return this.recordCount ;
   }
 
}