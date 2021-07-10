package it.sabd.uniroma2.app.util;

import java.io.BufferedWriter;
import java.io.FileWriter;
import java.io.IOException;
import java.io.PrintWriter;
import java.util.ArrayList;

public class CSVWriter {

    public void write(ArrayList<String> texts){

        for(String text : texts) {

            String[] fileWrite = text.split(Constants.OUTPUT_DIVIDER);
            String fileName = fileWrite[0];
            String textToAppend = fileWrite[1];

            try (FileWriter fw = new FileWriter(fileName, true);
                 BufferedWriter bw = new BufferedWriter(fw);
                 PrintWriter out = new PrintWriter(bw)) {

                out.println(textToAppend);

            } catch (IOException e) {
                System.out.println("Could not append to file: " + text);
                e.printStackTrace();

            }
        }
    }

}
