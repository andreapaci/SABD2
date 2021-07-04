package it.sabd.uniroma2.kafkaclient;

import java.io.*;
import java.text.SimpleDateFormat;
import java.util.*;

// CSV Parser to Load
public class CSVParser {

    private String filePath;
    private SimpleDateFormat dateDashParser;
    private SimpleDateFormat dateSlashParser;
    private SimpleDateFormat dateFormatter;


    public CSVParser(String filePath){
        this.filePath = filePath;

        dateDashParser = new SimpleDateFormat("dd-MM-yy HH:mm");
        dateSlashParser = new SimpleDateFormat("dd/MM/yy HH:mm");
        dateFormatter = new SimpleDateFormat("yyyy/MM/dd HH:mm");
    }


    public List<String[]> parseAndSort(boolean saveFile) {


        List<String[]> data = null;

        System.out.println("Parsing file: " + filePath);

        //Parsing file
        try { data = parse(); }
        catch(FileNotFoundException e) {

            System.out.println("File \"" + filePath + "\" not found.");
            e.printStackTrace();

            System.exit(-1);
        }
        catch(IOException e) {

            System.out.println("Error reading new line.");
            e.printStackTrace();

            System.exit(-1);
        }


        //Sorting file
        System.out.println("Sorting file.");
        data = sort(data);


        if(saveFile) {
            System.out.println("Saving dataset to a new CSV.");
            saveDataset(data);
        }

        return data;


    }

    private List<String[]> parse() throws IOException {

        BufferedReader reader = new BufferedReader(new FileReader(filePath));
        List<String[]> dataset = new ArrayList<String[]>();

        //Skip first line
        String line = reader.readLine();

        while ((line = reader.readLine()) != null) {

            String[] allFields = getLineFields(line);

            String key = allFields[7];

            List<String> fields = new ArrayList<String>();



            fields.add(key);
            fields.addAll(Arrays.asList(Arrays.copyOfRange(allFields, 0, 7)));
            fields.addAll(Arrays.asList(Arrays.copyOfRange(allFields, 8, 11)));

            dataset.add(fields.toArray(new String[0]));


        }
        reader.close();

        return dataset;

    }

    private List<String[]> sort(List<String[]> data) {

        Collections.sort(data, new Comparator<String[]>() {
            @Override
            public int compare(String[] lhs, String[] rhs) {
                return lhs[0].compareTo(rhs[0]);
            }
        });

        return data;

    }


    //Write dataset to new file
    private void saveDataset(List<String[]> dataset){

        try{

            BufferedWriter writer = new BufferedWriter(new FileWriter("ordered_dataset.csv"));


            writer.write("TIMESTAMP,SHIP_ID,SHIPTYPE,SPEED,LON,LAT,COURSE,HEADING,DEPARTURE_PORT_NAME,REPORTED_DRAUGHT,TRIP_ID\n");

            for(String[] data : dataset){

                String row = data[0];

                for(int i = 1; i < data.length; i++){
                    row += "," + data[i];
                }

                writer.write(row + "\n");
            }

            writer.close();

        } catch (Exception e) { e.printStackTrace(); }
    }










    //Get the field to sort on
    private String[] getLineFields(String line) {
        String[] fields =  line.split(",");

        fields[7] = timestampFormatter(fields[7]);

        return fields;
    }

    //Format dates to the same format yyyy/mm/dd hh:mm
    private String timestampFormatter(String date){

        Date parsedDate = null;

        if(date.contains("-")) {
            //Parsing dash-formatted date
            try {
                parsedDate = dateDashParser.parse(date);
            } catch (Exception e) {
                System.out.println("Data \"" + date + "\" not parseable");
                e.printStackTrace();
            }
        }
        else if(date.contains("/")) {
            //Parsing slash-formatted date
            try {
                parsedDate = dateSlashParser.parse(date);
            } catch (Exception e) {
                System.out.println("Data \"" + date + "\" not parseable");
                e.printStackTrace();
            }
        }
        else {
            System.out.println("Data \"" + date + "\" not in a readable format");
            System.exit(-1);
        }


        return dateFormatter.format(parsedDate);
    }


}
