/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package com.plesba.datamanager;

import java.io.IOException;

/**
 *
 * @author renee
 */
public class DataManager {
    

public static void main(String[] args) throws IOException {

        System.out.println("Starting DataManager main");
        String propertiesFile = null;
        String databasePropertiesFile = null;
        if (args.length < 1) {
            System.err.println("Usage: java " + DataManager.class.getName() + " <propertiesFile>");
            System.exit(1);
        } else if (args.length == 1) {
            propertiesFile = args[0];
            System.out.println("Properties file: " + propertiesFile);
        } else if (args.length == 2) {
            propertiesFile = args[0];
            System.out.println("Properties file: " + propertiesFile);
            databasePropertiesFile = args[1];
            System.out.println("Database Properties file: " + databasePropertiesFile);
        }
         
        dbProps = new DBProperties(databasePropertiesFile);
}

