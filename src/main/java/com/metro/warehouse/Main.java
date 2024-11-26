package com.metro.warehouse;

import com.metro.warehouse.meshjoin.MeshJoin;

public class Main {
    public static void main(String[] args) {
        System.out.println("Starting METRO Data Warehouse ETL Process...");
        
        MeshJoin meshJoin = new MeshJoin();
        
        try {
            // Start the MESHJOIN process
            meshJoin.start();
            
            // Let it run for a while (you can adjust this time or implement a different stopping condition)
            Thread.sleep(60000); // Run for 1 minute
            
            // Shutdown gracefully
            meshJoin.shutdown();
            
            System.out.println("ETL Process completed successfully!");
            
        } catch (Exception e) {
            System.err.println("Error during ETL process: " + e.getMessage());
            e.printStackTrace();
        }
    }
}
