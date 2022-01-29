drop schema if exists dwhproject;
create schema dwhproject;
use dwhproject;
-- Create the DWH
drop table if exists Transactions_Fact;
drop table if exists Store_Dim;
drop table if exists Date_Dim;
drop table if exists Supplier_Dim;
drop table if exists Product_Dim;
drop table if exists Customer_Dim;

CREATE table Store_Dim(
	StoreID VARCHAR(4) NOT NULL,
    StoreName VARCHAR(50),
    CONSTRAINT Store_pk PRIMARY KEY (StoreID)
);
CREATE table Date_Dim(
	DateID date NOT NULL,
    DayWeek varchar(10),
    Month int,
    Quarter int,
    Year int,
    CONSTRAINT Date_pk PRIMARY KEY (DateID)
);
CREATE table Supplier_Dim(
	SupplierID VARCHAR(6) NOT NULL,
    SupplierName VARCHAR(50),
    CONSTRAINT Supplier_pk PRIMARY KEY (SupplierID)
);
CREATE table Product_Dim(
	ProductID VARCHAR(7) NOT NULL,
    ProductName VARCHAR(50),
    CONSTRAINT Product_pk PRIMARY KEY (ProductID)
);
CREATE table Customer_Dim(
	CustomerID VARCHAR(6) NOT NULL,
    CustomerName VARCHAR(50),
    CONSTRAINT Customer_pk PRIMARY KEY (CustomerID)
);
-- FACT TABLE
CREATE table Transactions_Fact(
	TransactionID INT NOT NULL,
	StoreID VARCHAR(4) NOT NULL,
	ProductID VARCHAR(7) NOT NULL,
    SupplierID VARCHAR(6) NOT NULL,
    DateID date NOT NULL,
    CustomerID VARCHAR(6) NOT NULL,
    -- measures
    Total_Sale float,
    Quantity int,
    CONSTRAINT transFact_pk PRIMARY KEY (TransactionID),
	CONSTRAINT transStore_fk FOREIGN KEY(StoreID) References Store_Dim(StoreID),
    CONSTRAINT transProduct_fk FOREIGN KEY(ProductID) References Product_Dim(ProductID),
    CONSTRAINT transCustomer_fk FOREIGN KEY(CustomerID) References Customer_Dim(CustomerID),
    CONSTRAINT transDate_fk FOREIGN KEY(DateID) References Date_Dim(DateID),
    CONSTRAINT transSupplier_fk FOREIGN KEY(SupplierID) References Supplier_Dim(SupplierID)
); 
