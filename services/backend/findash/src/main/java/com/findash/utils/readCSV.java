package com.findash.utils;

import com.opencsv.CSVReader;
import com.opencsv.exceptions.CsvValidationException;
import java.io.FileReader;
import java.io.IOException;


public class readCSV {
    public static String[] read(String[] args) {
        String filePath = "C:/Users/Joseph/source/repos/FinDash/services/backend/findash/src/main/java/com/findash/utils/extract_config.csv";
        String[] headers = {};
        try (CSVReader read = new CSVReader(new FileReader(filePath))) {
            String[] nextLine;
            if ((nextLine = read.readNext()) != null) {
                System.out.println(String.join(",", nextLine));
                headers = nextLine;
            }
        } catch(IOException | CsvValidationException e) {
            e.printStackTrace();
        }
        System.out.println("CSV Headers: ");
        for (String header : headers) {
            System.out.println(header);
        }
        return headers;
    }
}
