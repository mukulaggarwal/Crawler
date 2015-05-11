package crawler;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileReader;
import java.io.FileWriter;
import java.io.IOException;

	public  class Test
    {
     public static void main(String args[])
         {
         try
             {
             File file = new File("file.txt");
             BufferedReader reader = new BufferedReader(new FileReader(file));
             String line = "", oldtext = "";
             while((line = reader.readLine()) != null)
                 {
                 oldtext += line + "";
             }
             reader.close();
             // replace a word in a file
             //String newtext = oldtext.replaceAll("drink", "Love");
 
             //To replace a line in a file
             String newtext = oldtext.replaceAll("This is test string 20000", "blah blah blah");
 
             FileWriter writer = new FileWriter("file.txt");
             writer.write(newtext);writer.close();
         }
         catch (IOException ioe)
             {
             ioe.printStackTrace();
         }
     }
}

