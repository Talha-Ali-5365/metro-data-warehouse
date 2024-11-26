package com.metro.warehouse.meshjoin;

import com.metro.warehouse.model.*;
import java.util.*;
import java.util.concurrent.*;
import java.sql.*;
import java.time.LocalDateTime;

public class MeshJoin {
    private final int CHUNK_SIZE = 100; // Size of each stream chunk
    private final int DISK_BUFFER_SIZE = 20; // Number of master data records to load at once
    private final String DB_URL = "jdbc:mysql://127.0.0.1:3306/metro_dw";
    private final String USER = "root";
    private final String PASS = "5365";

    private final Queue<List<Transaction>> streamQueue;
    private final Map<Integer, Customer> customerBuffer;
    private final Map<Integer, Product> productBuffer;
    private final ExecutorService executorService;

    public MeshJoin() {
        this.streamQueue = new ConcurrentLinkedQueue<>();
        this.customerBuffer = new ConcurrentHashMap<>();
        this.productBuffer = new ConcurrentHashMap<>();
        this.executorService = Executors.newFixedThreadPool(3); // 3 threads: stream reader, join processor, warehouse writer
    }

    public void start() {
        // Start stream reader thread
        executorService.submit(this::streamReader);
        
        // Start join processor thread
        executorService.submit(this::joinProcessor);
        
        // Start warehouse writer thread
        executorService.submit(this::warehouseWriter);
    }

    private void streamReader() {
        try (Connection conn = DriverManager.getConnection(DB_URL, USER, PASS)) {
            String sql = "SELECT * FROM transactions ORDER BY ORDER_DATE";
            try (PreparedStatement stmt = conn.prepareStatement(sql)) {
                ResultSet rs = stmt.executeQuery();
                
                List<Transaction> chunk = new ArrayList<>();
                while (rs.next()) {
                    Transaction transaction = new Transaction(
                        rs.getInt("ORDER_ID"),
                        rs.getTimestamp("ORDER_DATE").toLocalDateTime(),
                        rs.getInt("PRODUCT_ID"),
                        rs.getInt("CUSTOMER_ID"),
                        rs.getInt("QUANTITY"),
                        rs.getInt("TIME_ID")
                    );
                    
                    chunk.add(transaction);
                    if (chunk.size() >= CHUNK_SIZE) {
                        streamQueue.offer(new ArrayList<>(chunk));
                        chunk.clear();
                        Thread.sleep(100); // Simulate stream delay
                    }
                }
                
                if (!chunk.isEmpty()) {
                    streamQueue.offer(chunk);
                }
            }
        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    private void loadMasterData() {
        try (Connection conn = DriverManager.getConnection(DB_URL, USER, PASS)) {
            // Load customers
            String customerSql = "SELECT * FROM CUSTOMERS";
            try (PreparedStatement stmt = conn.prepareStatement(customerSql)) {
                ResultSet rs = stmt.executeQuery();
                while (rs.next()) {
                    Customer customer = new Customer(
                        rs.getInt("CUSTOMER_ID"),
                        rs.getString("CUSTOMER_NAME"),
                        rs.getString("GENDER")
                    );
                    customerBuffer.put(customer.getCustomerId(), customer);
                }
            }

            // Load products
            String productSql = "SELECT * FROM PRODUCTS";
            try (PreparedStatement stmt = conn.prepareStatement(productSql)) {
                ResultSet rs = stmt.executeQuery();
                while (rs.next()) {
                    Product product = new Product(
                        rs.getInt("PRODUCT_ID"),
                        rs.getString("PRODUCT_NAME"),
                        rs.getDouble("PRODUCT_PRICE"),
                        rs.getInt("SUPPLIER_ID"),
                        rs.getString("SUPPLIER_NAME"),
                        rs.getInt("STORE_ID"),
                        rs.getString("STORE_NAME")
                    );
                    productBuffer.put(product.getProductId(), product);
                }
            }
        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    private void joinProcessor() {
        loadMasterData(); // Load master data first
        
        while (true) {
            List<Transaction> chunk = streamQueue.poll();
            if (chunk == null) {
                try {
                    Thread.sleep(100);
                    continue;
                } catch (InterruptedException e) {
                    break;
                }
            }

            List<WarehouseRecord> enrichedRecords = new ArrayList<>();
            for (Transaction transaction : chunk) {
                Customer customer = customerBuffer.get(transaction.getCustomerId());
                Product product = productBuffer.get(transaction.getProductId());
                
                if (customer != null && product != null) {
                    double sale = transaction.getQuantity() * product.getProductPrice();
                    
                    WarehouseRecord record = new WarehouseRecord(
                        transaction.getOrderId(),
                        transaction.getOrderDate(),
                        product.getProductId(),
                        customer.getCustomerId(),
                        customer.getCustomerName(),
                        customer.getGender(),
                        transaction.getQuantity(),
                        product.getProductName(),
                        product.getProductPrice(),
                        product.getSupplierId(),
                        product.getSupplierName(),
                        product.getStoreId(),
                        product.getStoreName(),
                        sale
                    );
                    enrichedRecords.add(record);
                }
            }
            
            if (!enrichedRecords.isEmpty()) {
                insertIntoWarehouse(enrichedRecords);
            }
        }
    }

    private void insertIntoWarehouse(List<WarehouseRecord> records) {
        try (Connection conn = DriverManager.getConnection(DB_URL, USER, PASS)) {
            String sql = "INSERT INTO WAREHOUSE_SALES (ORDER_ID, ORDER_DATE, PRODUCT_ID, CUSTOMER_ID, " +
                        "CUSTOMER_NAME, GENDER, QUANTITY, PRODUCT_NAME, PRODUCT_PRICE, SUPPLIER_ID, " +
                        "SUPPLIER_NAME, STORE_ID, STORE_NAME, SALE) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)";
            
            try (PreparedStatement stmt = conn.prepareStatement(sql)) {
                for (WarehouseRecord record : records) {
                    stmt.setInt(1, record.getOrderId());
                    stmt.setTimestamp(2, Timestamp.valueOf(record.getOrderDate()));
                    stmt.setInt(3, record.getProductId());
                    stmt.setInt(4, record.getCustomerId());
                    stmt.setString(5, record.getCustomerName());
                    stmt.setString(6, record.getGender());
                    stmt.setInt(7, record.getQuantity());
                    stmt.setString(8, record.getProductName());
                    stmt.setDouble(9, record.getProductPrice());
                    stmt.setInt(10, record.getSupplierId());
                    stmt.setString(11, record.getSupplierName());
                    stmt.setInt(12, record.getStoreId());
                    stmt.setString(13, record.getStoreName());
                    stmt.setDouble(14, record.getSale());
                    
                    stmt.addBatch();
                }
                stmt.executeBatch();
            }
        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    private void warehouseWriter() {
        // This thread is currently integrated into the joinProcessor
        // We can add additional warehouse writing logic here if needed
    }

    public void shutdown() {
        executorService.shutdown();
        try {
            if (!executorService.awaitTermination(60, TimeUnit.SECONDS)) {
                executorService.shutdownNow();
            }
        } catch (InterruptedException e) {
            executorService.shutdownNow();
        }
    }
}
