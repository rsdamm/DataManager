
package com.plesba.datamanager.source;

import com.opencsv.CSVReader;
import java.io.FileReader;
import java.io.IOException;
import java.io.PipedOutputStream;
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

        System.out.println("CSVSource started processing.....");
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

          System.out.println("CSVSource Writing record to stream---> "+ recordStringBuffer);

          recordCount++;
          recordStringBuffer.setLength(0);
        }

        System.out.println("CSVSource finished processing.....");
        outputStream.close();

       } catch (IOException e) {
                    throw new RuntimeException(e);
       }
   }
   public int getReadCount ()    {
        return this.recordCount ;
   }
 
}