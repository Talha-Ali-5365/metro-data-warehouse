package com.metro.warehouse.model;

import java.time.LocalDateTime;

public class WarehouseRecord {
    private int orderId;
    private LocalDateTime orderDate;
    private int productId;
    private int customerId;
    private String customerName;
    private String gender;
    private int quantity;
    private String productName;
    private double productPrice;
    private int supplierId;
    private String supplierName;
    private int storeId;
    private String storeName;
    private double sale;

    public WarehouseRecord(int orderId, LocalDateTime orderDate, int productId, int customerId,
                          String customerName, String gender, int quantity, String productName,
                          double productPrice, int supplierId, String supplierName,
                          int storeId, String storeName, double sale) {
        this.orderId = orderId;
        this.orderDate = orderDate;
        this.productId = productId;
        this.customerId = customerId;
        this.customerName = customerName;
        this.gender = gender;
        this.quantity = quantity;
        this.productName = productName;
        this.productPrice = productPrice;
        this.supplierId = supplierId;
        this.supplierName = supplierName;
        this.storeId = storeId;
        this.storeName = storeName;
        this.sale = sale;
    }

    // Getters
    public int getOrderId() { return orderId; }
    public LocalDateTime getOrderDate() { return orderDate; }
    public int getProductId() { return productId; }
    public int getCustomerId() { return customerId; }
    public String getCustomerName() { return customerName; }
    public String getGender() { return gender; }
    public int getQuantity() { return quantity; }
    public String getProductName() { return productName; }
    public double getProductPrice() { return productPrice; }
    public int getSupplierId() { return supplierId; }
    public String getSupplierName() { return supplierName; }
    public int getStoreId() { return storeId; }
    public String getStoreName() { return storeName; }
    public double getSale() { return sale; }
}
