
package com.plesba.datamanager.source;

import com.opencsv.CSVReader;
import java.io.FileReader;
import java.io.IOException;
import java.io.PipedOutputStream;

public final class CSVSource
{    
    private final String fileToRead;
    
    private CSVReader reader = null; 
    private PipedOutputStream outputStream = null;
    private int recordCount = 0; 
    private String [] nextLine = null;  
    private byte[] theByteArray = null;
    private StringBuilder recordStringBuffer;
    private int i=0;
    
    public CSVSource ( String rfn, PipedOutputStream oStream ) {
       fileToRead = rfn;  
       outputStream = oStream;
       csvreaderSetup();
    
    }
    public CSVSource() {
        this.recordStringBuffer = null;
            fileToRead = null; 
            outputStream = null;
            reader = null; 
    }
 
    public void csvreaderSetup() {

        try {
             //Get the CSVReader instance specifying the delimiter to be used
            reader = new CSVReader(new FileReader(fileToRead),',');              
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

          System.out.println("Writing record to stream---> "+ recordStringBuffer); 
 
          recordCount++;
          recordStringBuffer.setLength(0);
        } 
        
        System.out.println("end of data stream"+ recordStringBuffer);
        outputStream.close();
        
       } catch (IOException e) {
                    throw new RuntimeException(e);
       }
   }
   public int getReadCount ()    {
        return this.recordCount ;
   }
 
}