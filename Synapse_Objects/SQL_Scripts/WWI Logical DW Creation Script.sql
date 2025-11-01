-- || DATABASE SETUP ||

-- Create database and alter to handle UTF8
CREATE DATABASE WWILogicalDW;
GO
ALTER DATABASE WWILogicalDW COLLATE Latin1_General_100_BIN2_UTF8;
GO

-- Create master key and configure credentials to data lake
CREATE MASTER KEY ENCRYPTION BY PASSWORD = '<YOUR_STRONG_PASSWORD>';

CREATE DATABASE SCOPED CREDENTIAL SynapseUserIdentity
    WITH IDENTITY = 'User Identity';

-- Create external data source and external file format (for parquet writes to data lake)
CREATE EXTERNAL DATA SOURCE SourceDataLake
    WITH(
        CREDENTIAL=SynapseUserIdentity,
        LOCATION='<YOUR_ADLS_PATH>'
    );
GO

CREATE EXTERNAL FILE FORMAT ParquetFormat
    WITH(
        FORMAT_TYPE = PARQUET
    );
GO

-- Create new schemas for ldw views and staging external tables, respectively
CREATE SCHEMA LDW AUTHORIZATION dbo;
GO
CREATE SCHEMA STG AUTHORIZATION dbo;
GO

-- || CREATE INITIAL VIEWS OVER ALL RAW HISTORICAL DATA ||

-- SalesOrders
CREATE VIEW LDW.vwSalesOrders AS
    SELECT *,
           CAST(rows.filepath(1) AS DATE) AS FilePathDate
    FROM OPENROWSET (
        BULK 'Sales_Order/OrderDatePartition=*/*.txt', -- once new raw orders come in, those will be captured here too
        DATA_SOURCE = 'SourceDataLake',
        PARSER_VERSION = '2.0',
        FORMAT = 'CSV',
        FIELDTERMINATOR = ',',
        STRING_DELIMITER = '"',
        HEADER_ROW = TRUE
    )
    WITH(
         OrderID INT,
         CustomerID SMALLINT,
         SalespersonPersonID SMALLINT,
         PickedByPersonID SMALLINT,
         ContactPersonID SMALLINT,
         BackorderOrderID INT,
         OrderDate DATE,
         ExpectedDeliveryDate DATETIME2,
         CustomerPurchaseOrderNumber INT, 
         IsUndersupplyBackordered BIT,
         Comments VARCHAR(250),	
         DeliveryInstructions VARCHAR(250),
         InternalComments VARCHAR(250),
         PickingCompletedWhen DATETIME2,	
         LastEditedBy SMALLINT,
         LastEditedWhen DATETIME2
    ) AS rows;
GO

-- SalesOrderLines
CREATE VIEW LDW.vwSalesOrderLines AS
    SELECT *,
           CAST(rows.filepath(1) AS DATE) AS FilePathDate
    FROM OPENROWSET(
        BULK 'Sales_Orderline/OrderDate=*/*.txt', -- once new raw orderlines come in, those will be captured here too
        DATA_SOURCE = 'SourceDataLake',
        PARSER_VERSION = '2.0',
        FORMAT = 'CSV',
        FIELDTERMINATOR = ',',
        STRING_DELIMITER = '"',
        HEADER_ROW = TRUE
    )
    WITH(
        OrderLineID INT,
        OrderID INT,
        StockItemID SMALLINT,
        PackageTypeID TINYINT,
        Quantity SMALLINT,
        UnitPrice DECIMAL(6, 2),
        TaxRate DECIMAL(5, 2),
        PickedQuantity SMALLINT,
        PickingCompletedWhen DATETIME2,
        LastEditedBy SMALLINT,
        LastEditedWhen DATETIME2
    ) AS rows;
GO

-- Customers
CREATE VIEW LDW.vwCustomers AS
    SELECT *
    FROM OPENROWSET(
        BULK 'Initial_Dims/Sales_Customers/*.txt',
        DATA_SOURCE = 'SourceDataLake',
        PARSER_VERSION = '2.0',
        FORMAT = 'CSV',
        FIELDTERMINATOR = ',',
        STRING_DELIMITER = '"',
        HEADER_ROW = TRUE
    )
    WITH(
     CustomerID SMALLINT,
     CustomerName VARCHAR(100),
     BillToCustomerID SMALLINT,
     CustomerCategoryID TINYINT,
     BuyingGroupID DECIMAL(4, 1),
     PrimaryContactPersonID SMALLINT,
     AlternateContactPersonID DECIMAL(6, 1),
     DeliveryMethodID TINYINT,
     DeliveryCityID INT,
     PostalCityID INT,
     CreditLimit DECIMAL(10, 2),
     AccountOpenedDate DATETIME2,
     StandardDiscountPercentage DECIMAL(5, 2),
     IsStatementSent BIT,
     IsOnCreditHold BIT,
     PaymentDays TINYINT,
     PhoneNumber VARCHAR(20),
     FaxNumber VARCHAR(20),
     DeliveryRun VARCHAR(50),
     RunPosition VARCHAR(50),
     WebsiteURL VARCHAR(100),
     DeliveryAddressLine1 VARCHAR(100),
     DeliveryAddressLine2 VARCHAR(100),
     DeliveryPostalCode VARCHAR(5),
     DeliveryLocation VARCHAR(50),
     PostalAddressLine1 VARCHAR(50),
     PostalAddressLine2 VARCHAR(50),
     PostalPostalCode VARCHAR(5),
     LastEditedBy SMALLINT,
     ValidFrom DATETIME2,
     ValidTo DATETIME2
    ) AS rows;
GO

-- Suppliers
CREATE VIEW LDW.vwSuppliers AS
    SELECT *
    FROM OPENROWSET (
        BULK 'Initial_Dims/Purchasing_Suppliers/*.txt',
        DATA_SOURCE = 'SourceDataLake',
        PARSER_VERSION = '2.0',
        FORMAT = 'CSV',
        FIELDTERMINATOR = ',',
        STRING_DELIMITER = '"',
        HEADER_ROW = TRUE
    )
    WITH(
         SupplierID TINYINT,
         SupplierName VARCHAR(100),
         SupplierCategoryID TINYINT,
         PrimaryContactPerson SMALLINT,
         AlternateContactPersonID SMALLINT,
         DeliveryMethodID TINYINT,
         DeliveryCityID INT,
         PostalCityID INT,
         SupplierReference VARCHAR(20),
         BankAccountName VARCHAR(50),
         BankAccountBranch VARCHAR(50),
         BankAccountCode VARCHAR(6),
         BankAccountNumber VARCHAR(10),
         BankInternationalCode VARCHAR(5),
         PaymentDays TINYINT,
         InternalComments VARCHAR(100),
         PhoneNumber VARCHAR(20),
         FaxNumber VARCHAR(20),
         WebsiteURL VARCHAR(100),
         DeliveryAddressLine1 VARCHAR(100),
         DeliveryAddressLine2 VARCHAR(100),
         DeliveryPostalCode VARCHAR(5),
         DeliveryLocation VARCHAR(50),
         PostalAddressLine1 VARCHAR(50),
         PostalAddressLine2 VARCHAR(50),
         PostalPostalCode VARCHAR(5),
         LastEditedBy SMALLINT,
         ValidFrom DATETIME2,
         ValidTo DATETIME2
    ) AS rows;
  GO

-- Cities
CREATE VIEW LDW.vwCities AS
    SELECT *
    FROM OPENROWSET (
        BULK 'Initial_Dims/Application_Cities/*.txt',
        DATA_SOURCE = 'SourceDataLake',
        PARSER_VERSION = '2.0',
        FORMAT = 'CSV',
        FIELDTERMINATOR = ',',
        STRING_DELIMITER = '"',
        HEADER_ROW = TRUE
    )
    WITH(
        CityID INT,
        CityName VARCHAR(50),
        StateProvinceID TINYINT,
        Location VARCHAR(50),
        LatestRecordedPopulation DECIMAL(9, 1),
        LastEditedBy SMALLINT,
        ValidFrom DATETIME2,
        ValidTo DATETIME2
    ) AS rows;
GO

-- Countries
CREATE VIEW LDW.vwCountries AS
    SELECT *
    FROM OPENROWSET (
        BULK 'Initial_Dims/Application_Countries/*.txt',
        DATA_SOURCE = 'SourceDataLake',
        PARSER_VERSION = '2.0',
        FORMAT = 'CSV',
        FIELDTERMINATOR = ',',
        STRING_DELIMITER = '"',
        HEADER_ROW = TRUE
    )
    WITH(
        CountryID TINYINT,
        CountryName VARCHAR(50),
        FormalName VARCHAR(100),
        IsoAlpha3Code VARCHAR(3),
        IsoNumericCode SMALLINT,
        CountryType VARCHAR(25),
        LatestRecordedPopulation INT,
        Continent VARCHAR(25),
        Region VARCHAR(25),
        Subregion VARCHAR(30),
        LastEditedBy SMALLINT,
        ValidFrom DATETIME2,
        ValidTo DATETIME2
    ) AS rows;
GO

-- StateProvinces
CREATE VIEW LDW.vwStateProvinces AS
    SELECT *
    FROM OPENROWSET (
        BULK 'Initial_Dims/Application_StateProvinces/*.txt',
        DATA_SOURCE = 'SourceDataLake',
        PARSER_VERSION = '2.0',
        FORMAT = 'CSV',
        FIELDTERMINATOR = ',',
        STRING_DELIMITER = '"',
        HEADER_ROW = TRUE
    )
    WITH(
        StateProvinceID TINYINT,
        StateProvinceCode VARCHAR(2),
        StateProvinceName VARCHAR(30),
        CountryID TINYINT,
        SalesTerritory VARCHAR(25),
        LatestRecordedPopulation INT,
        LastEditedBy SMALLINT,
        ValidFrom DATETIME2,
        ValidTo DATETIME2
    ) AS rows;
GO

-- DeliveryMethods
CREATE VIEW LDW.vwDeliveryMethods AS
    SELECT *
    FROM OPENROWSET (
        BULK 'Initial_Dims/Application_DeliveryMethods/*.txt',
        DATA_SOURCE = 'SourceDataLake',
        PARSER_VERSION = '2.0',
        FORMAT = 'CSV',
        FIELDTERMINATOR = ',',
        STRING_DELIMITER = '"',
        HEADER_ROW = TRUE
    ) AS rows;
GO

-- People
CREATE VIEW LDW.vwPeople AS
    SELECT *
    FROM OPENROWSET (
        BULK 'Initial_Dims/Application_People/*.txt',
        DATA_SOURCE = 'SourceDataLake',
        PARSER_VERSION = '2.0',
        FORMAT = 'CSV',
        FIELDTERMINATOR = ',',
        STRING_DELIMITER = '"',
        HEADER_ROW = TRUE
    )
    WITH(
        PersonID SMALLINT,
        FullName VARCHAR(50),
        PreferredName VARCHAR(50),
        SearchName VARCHAR(50),
        IsPermittedToLogon BIT,
        LogonName VARCHAR(50),
        IsExternalLogonProvider BIT,
        IsSystemUser BIT,
        IsEmployee BIT,
        IsSalesperson BIT,
        PhoneNumber VARCHAR(20),
        FaxNumber VARCHAR(20),
        EmailAddress VARCHAR(50)
    ) AS rows;
GO

-- SupplierCategories
CREATE VIEW LDW.vwSupplierCategories AS
    SELECT *
    FROM OPENROWSET (
        BULK 'Initial_Dims/Purchasing_SupplierCategories/*.txt',
        DATA_SOURCE = 'SourceDataLake',
        PARSER_VERSION = '2.0',
        FORMAT = 'CSV',
        FIELDTERMINATOR = ',',
        STRING_DELIMITER = '"',
        HEADER_ROW = TRUE
    ) AS rows;
GO

-- BuyingGroup
CREATE VIEW LDW.vwBuyingGroups AS
    SELECT *
    FROM OPENROWSET (
        BULK 'Initial_Dims/Sales_BuyingGroups/*.txt',
        DATA_SOURCE = 'SourceDataLake',
        PARSER_VERSION = '2.0',
        FORMAT = 'CSV',
        FIELDTERMINATOR = ',',
        STRING_DELIMITER = '"',
        HEADER_ROW = TRUE
    ) AS rows;
GO

-- CustomerCategories
CREATE VIEW LDW.vwCustomerCategories AS
    SELECT *
    FROM OPENROWSET (
        BULK 'Initial_Dims/Sales_CustomerCategories/*.txt',
        DATA_SOURCE = 'SourceDataLake',
        PARSER_VERSION = '2.0',
        FORMAT = 'CSV',
        FIELDTERMINATOR = ',',
        STRING_DELIMITER = '"',
        HEADER_ROW = TRUE
    ) AS rows;
GO

-- Colors
CREATE VIEW LDW.vwColors AS
    SELECT *
    FROM OPENROWSET (
        BULK 'Initial_Dims/Warehouse_Colors/*.txt',
        DATA_SOURCE = 'SourceDataLake',
        PARSER_VERSION = '2.0',
        FORMAT = 'CSV',
        FIELDTERMINATOR = ',',
        STRING_DELIMITER = '"',
        HEADER_ROW = TRUE
    ) AS rows;
GO

-- PackageTypes
CREATE VIEW LDW.vwPackageTypes AS
    SELECT *
    FROM OPENROWSET (
        BULK 'Initial_Dims/Warehouse_PackageTypes/*.txt',
        DATA_SOURCE = 'SourceDataLake',
        PARSER_VERSION = '2.0',
        FORMAT = 'CSV',
        FIELDTERMINATOR = ',',
        STRING_DELIMITER = '"',
        HEADER_ROW = TRUE
    ) AS rows;
GO

--StockItems
CREATE VIEW LDW.vwStockItems AS
    SELECT *
    FROM OPENROWSET (
        BULK 'Initial_Dims/Warehouse_StockItems/*.txt',
        DATA_SOURCE = 'SourceDataLake',
        PARSER_VERSION = '2.0',
        FORMAT = 'CSV',
        FIELDTERMINATOR = ',',
        STRING_DELIMITER = '"',
        HEADER_ROW = TRUE
    )
    WITH(
        StockItemID SMALLINT,
        StockItemName VARCHAR(100),
        SupplierID TINYINT,
        ColorID DECIMAL(4, 1),
        UnitPackageID TINYINT,
        OuterPackageID TINYINT,
        Brand VARCHAR(50),
        Size VARCHAR(25),
        LeadTimeDays TINYINT,
        QuantityPerOuter TINYINT,
        IsChillerStock BIT,
        Barcode VARCHAR(20),
        TaxRate DECIMAL(5, 2),
        UnitPrice DECIMAL(6, 2),
        RecommendedRetailPrice DECIMAL(6, 2),
        TypicalWeightPerUnit DECIMAL(5, 2),
        MarketingComments VARCHAR(250),
        InternalComments VARCHAR(250),
        Photo NVARCHAR(1000),
        SearchDetails VARCHAR(150)
    ) AS rows;
GO

-- || WRITE INITIAL CONFORMED DIM TABLES TO LAKE ||

-- DimCustomers
CREATE EXTERNAL TABLE STG.DimCustomers
    WITH(
        LOCATION = 'Conformed/Dimensions/Dim_Customers/01', -- Initial write only; incremental writes handled by separate usp script
        DATA_SOURCE = SourceDataLake,
        FILE_FORMAT = ParquetFormat
    )
    AS
    SELECT CAST(ROW_NUMBER() OVER(ORDER BY C.CustomerID) AS SMALLINT) AS CustomerKey,
           C.CustomerID,
           C.CustomerName,
           CC.CustomerCategoryName,
           BG.BuyingGroupName,
           D.DeliveryMethodName,
           C.CreditLimit,
           C.StandardDiscountPercentage,
           CI.CityName,
           S.StateProvinceName,
           S.SalesTerritory,
           CO.CountryName,
           CO.Continent,
           CO.Region,
           CO.Subregion,
           CAST(C.ValidFrom AS DATE) AS ValidFromDate
    FROM LDW.vwCustomers C
    LEFT JOIN LDW.vwCustomerCategories CC
        ON C.CustomerCategoryID = CC.CustomerCategoryID
    LEFT JOIN LDW.vwBuyingGroups BG
        ON C.BuyingGroupID = BG.BuyingGroupID
    LEFT JOIN LDW.vwCities CI
        ON C.DeliveryCityID = CI.CityID
    LEFT JOIN LDW.vwStateProvinces S
        ON CI.StateProvinceID = S.StateProvinceID
    LEFT JOIN LDW.vwCountries CO
        ON S.CountryID = CO.CountryID
    LEFT JOIN LDW.vwDeliveryMethods D
        ON C.DeliveryMethodID = D.DeliveryMethodID
    ORDER BY C.CustomerID;
GO

DROP EXTERNAL TABLE STG.DimCustomers;
GO

-- DimStockItems
CREATE EXTERNAL TABLE STG.DimStockItems
    WITH(
        LOCATION = 'Conformed/Dimensions/Dim_StockItems/01',
        DATA_SOURCE = SourceDataLake,
        FILE_FORMAT = ParquetFormat
    )
    AS
    SELECT CAST(ROW_NUMBER() OVER(ORDER BY SI.StockItemID) AS SMALLINT) AS StockItemKey,
           SI.StockItemID,
           C.ColorName,
           P.PackageTypeName,
           SI.Brand,
           SU.SupplierName,
           SI.LeadTimeDays,
           CAST('2013-01-01' AS DATE) AS ValidFromDate
    FROM LDW.vwStockItems SI
    LEFT JOIN LDW.vwColors C
        ON SI.ColorID = C.ColorID
    LEFT JOIN LDW.vwPackageTypes P
        ON SI.OuterPackageID = P.PackageTypeID
    LEFT JOIN LDW.vwSuppliers SU
        ON SI.SupplierID = SU.SupplierID
    ORDER BY SI.StockItemID;
GO

DROP EXTERNAL TABLE STG.DimStockItems
GO

-- DimSuppliers
CREATE EXTERNAL TABLE STG.DimSuppliers
    WITH(
        LOCATION = 'Conformed/Dimensions/Dim_Suppliers/01', -- initial write only; incremental writes handled by separate usp script
        DATA_SOURCE = SourceDataLake,
        FILE_FORMAT = ParquetFormat
    )
    AS
    SELECT CAST(ROW_NUMBER() OVER(ORDER BY SU.SupplierID) AS TINYINT) AS SupplierKey,
           SU.SupplierID,
           SU.SupplierName,
           SC.SupplierCategoryName,
           SU.PaymentDays,
           D.DeliveryMethodName,
           CI.CityName,
           SU.PhoneNumber,
           SU.BankAccountName,
           CAST(SU.ValidFrom AS DATE) AS ValidFromDate
    FROM LDW.vwSuppliers SU
    LEFT JOIN LDW.vwSupplierCategories SC
        ON SU.SupplierCategoryID = SC.SupplierCategoryID
    LEFT JOIN LDW.vwDeliveryMethods D
        ON SU.DeliveryMethodID = D.DeliveryMethodID
    LEFT JOIN LDW.vwCities CI
        ON SU.DeliveryCityID = CI.CityID
    ORDER BY SU.SupplierID;
GO

DROP EXTERNAL TABLE STG.DimSuppliers
GO

-- DimDate
DROP EXTERNAL TABLE STG.DimDate
CREATE EXTERNAL TABLE STG.DimDate
    WITH (
        LOCATION = 'Conformed/Dimensions/Dim_Date',
        DATA_SOURCE = SourceDataLake,
        FILE_FORMAT = ParquetFormat
    )
    AS
    SELECT CAST(DateKey AS INT) AS DateKey,
           CAST(Date AS DATE) AS Date,
           CAST(Day AS TINYINT) AS Day,
           CAST(WeekDay AS TINYINT) AS WeekDay,
           WeekDayName,
           CAST(Month AS TINYINT) AS Month,
           MonthName,
           CAST(Quarter AS TINYINT) AS Quarter,
           CAST(Year AS SMALLINT) AS Year
    FROM OPENROWSET(
        BULK 'Initial_Dims/Date_DateTable/*.csv',
        DATA_SOURCE = 'SourceDataLake',
        PARSER_VERSION = '2.0',
        FORMAT = 'CSV',
        FIELDTERMINATOR = '|',
        STRING_DELIMITER = '"',
        HEADER_ROW = TRUE
    ) AS rows
    ORDER BY Date;
GO

DROP EXTERNAL TABLE STG.DimDate
GO

-- || CREATE CLEANED VIEWS OVER FULL DIMS ||

-- DimCustomers
CREATE VIEW LDW.vwDimCustomers AS
    SELECT *
    FROM OPENROWSET(
        BULK 'Conformed/Dimensions/Dim_Customers/*/', 
        DATA_SOURCE = 'SourceDataLake',
        FORMAT = 'Parquet'
    ) AS rows;
GO

-- DimStockItems
CREATE VIEW LDW.vwDimStockItems AS
    SELECT *
    FROM OPENROWSET(
        BULK 'Conformed/Dimensions/Dim_StockItems/*/',
        DATA_SOURCE = 'SourceDataLake',
        FORMAT = 'Parquet'
    ) AS rows;
GO

-- DimSuppliers
CREATE VIEW LDW.vwDimSuppliers AS
    SELECT *
    FROM OPENROWSET(
        BULK 'Conformed/Dimensions/Dim_Suppliers/*/',
        DATA_SOURCE = 'SourceDataLake',
        FORMAT = 'Parquet'
    ) AS rows;
GO

-- DimDate
CREATE VIEW LDW.vwDimDate AS
    SELECT *
    FROM OPENROWSET(
        BULK 'Conformed/Dimensions/Dim_Date/*',
        DATA_SOURCE = 'SourceDataLake',
        FORMAT = 'Parquet'
    ) AS rows;
GO

-- || WRITE INITIAL CONFORMED FACT SALES TO LAKE ||

CREATE EXTERNAL TABLE STG.FactSales
    WITH(
        LOCATION = 'Conformed/Facts/Fact_Sales/Initial', -- Initial write only; incremental writes handled by separate usp script
        DATA_SOURCE = SourceDataLake,
        FILE_FORMAT = ParquetFormat
    )
    AS
    SELECT CAST(FORMAT(SO.OrderDate, 'yyyyMMdd') AS INT) AS OrderDateKey,
           C.CustomerKey,
           DSI.StockItemKey,
           SU.SupplierKey,
           SOL.OrderLineID,
           SO.OrderID,
           SOL.Quantity,
           SOL.UnitPrice
    FROM LDW.vwSalesOrderLines SOL -- combining sales order lines and sales order; will view at the sales order level
    JOIN LDW.vwSalesOrders SO
        ON SOL.OrderID = SO.OrderID
    LEFT JOIN LDW.vwDimCustomers C
        ON SO.CustomerID = C.CustomerID
    LEFT JOIN LDW.vwDimStockItems DSI
        ON SOL.StockItemID = DSI.StockItemID
    LEFT JOIN LDW.vwStockItems SI
        ON DSI.StockItemID = SI.StockItemID
    LEFT JOIN LDW.vwDimSuppliers SU
        ON SI.SupplierID = SU.SupplierID;
GO

DROP EXTERNAL TABLE STG.FactSales
GO

-- || CREATE CLEANED VIEW OVER FULL FACT SALES ||

CREATE VIEW LDW.vwFactSales AS
    SELECT *,
           CAST(rows.filepath(2) AS DATE) SalesOrderPathDate
    FROM OPENROWSET(
        BULK 'Conformed/Facts/Fact_Sales/*/*/*.parquet',
        DATA_SOURCE = 'SourceDataLake',
        FORMAT = 'Parquet'
    ) AS rows;
GO

-- || CREATE INITIAL VIEWS OVER RAW INCREMENTAL DIM DATA ||

--IncrementalCustomers
CREATE VIEW LDW.vwIncrementalCustomers AS
    SELECT *,
           CAST(rows.filepath(1) AS DATE) AS FilePathDate
    FROM OPENROWSET(
        BULK 'Changed_Dims/*/Sales_Customers/*.txt',
        DATA_SOURCE = 'SourceDataLake',
        PARSER_VERSION = '2.0',
        FORMAT = 'CSV',
        FIELDTERMINATOR = ',',
        STRING_DELIMITER = '"',
        HEADER_ROW = TRUE
    )
    WITH(
     CustomerID SMALLINT,
     CustomerName VARCHAR(100),
     BillToCustomerID SMALLINT,
     CustomerCategoryID TINYINT,
     BuyingGroupID DECIMAL(4, 1),
     PrimaryContactPersonID SMALLINT,
     AlternateContactPersonID DECIMAL(6, 1),
     DeliveryMethodID TINYINT,
     DeliveryCityID INT,
     PostalCityID INT,
     CreditLimit DECIMAL(10, 2),
     AccountOpenedDate DATETIME2,
     StandardDiscountPercentage DECIMAL(5, 2),
     IsStatementSent BIT,
     IsOnCreditHold BIT,
     PaymentDays TINYINT,
     PhoneNumber VARCHAR(20),
     FaxNumber VARCHAR(20),
     DeliveryRun VARCHAR(50),
     RunPosition VARCHAR(50),
     WebsiteURL VARCHAR(100),
     DeliveryAddressLine1 VARCHAR(100),
     DeliveryAddressLine2 VARCHAR(100),
     DeliveryPostalCode VARCHAR(5),
     DeliveryLocation VARCHAR(50),
     PostalAddressLine1 VARCHAR(50),
     PostalAddressLine2 VARCHAR(50),
     PostalPostalCode VARCHAR(5),
     LastEditedBy SMALLINT,
     ValidFrom DATETIME2,
     ValidTo DATETIME2
    ) AS rows;
GO

--IncrementalSuppliers
CREATE VIEW LDW.vwIncrementalSuppliers AS
    SELECT *,
           CAST(rows.filepath(1) AS DATE) AS FilePathDate
    FROM OPENROWSET(
        BULK 'Changed_Dims/*/Purchasing_Suppliers/*.txt',
        DATA_SOURCE = 'SourceDataLake',
        PARSER_VERSION = '2.0',
        FORMAT = 'CSV',
        FIELDTERMINATOR = ',',
        STRING_DELIMITER = '"',
        HEADER_ROW = TRUE
    )
    WITH(
         SupplierID TINYINT,
         SupplierName VARCHAR(100),
         SupplierCategoryID TINYINT,
         PrimaryContactPerson SMALLINT,
         AlternateContactPersonID SMALLINT,
         DeliveryMethodID TINYINT,
         DeliveryCityID INT,
         PostalCityID INT,
         SupplierReference VARCHAR(20),
         BankAccountName VARCHAR(50),
         BankAccountBranch VARCHAR(50),
         BankAccountCode VARCHAR(6),
         BankAccountNumber VARCHAR(10),
         BankInternationalCode VARCHAR(5),
         PaymentDays TINYINT,
         InternalComments VARCHAR(100),
         PhoneNumber VARCHAR(20),
         FaxNumber VARCHAR(20),
         WebsiteURL VARCHAR(100),
         DeliveryAddressLine1 VARCHAR(100),
         DeliveryAddressLine2 VARCHAR(100),
         DeliveryPostalCode VARCHAR(5),
         DeliveryLocation VARCHAR(50),
         PostalAddressLine1 VARCHAR(50),
         PostalAddressLine2 VARCHAR(50),
         PostalPostalCode VARCHAR(5),
         LastEditedBy SMALLINT,
         ValidFrom DATETIME2,
         ValidTo DATETIME2
    ) AS rows;
  GO

--IncrementalCities
CREATE VIEW LDW.vwIncrementalCities AS
    SELECT *,
           CAST(rows.filepath(1) AS DATE) AS FilePathDate
    FROM OPENROWSET(
        BULK 'Changed_Dims/*/Application_Cities/*.txt',
        DATA_SOURCE = 'SourceDataLake',
        PARSER_VERSION = '2.0',
        FORMAT = 'CSV',
        FIELDTERMINATOR = ',',
        STRING_DELIMITER = '"',
        HEADER_ROW = TRUE
    )
        WITH(
        CityID INT,
        CityName VARCHAR(50),
        StateProvinceID TINYINT,
        Location VARCHAR(50),
        LatestRecordedPopulation DECIMAL(9, 1),
        LastEditedBy SMALLINT,
        ValidFrom DATETIME2,
        ValidTo DATETIME2
    ) AS rows;
GO

--IncrementalCountries
CREATE VIEW LDW.vwIncrementalCountries AS
    SELECT *,
           CAST(rows.filepath(1) AS DATE) AS FilePathDate
    FROM OPENROWSET(
        BULK 'Changed_Dims/*/Application_Countries/*.txt',
        DATA_SOURCE = 'SourceDataLake',
        PARSER_VERSION = '2.0',
        FORMAT = 'CSV',
        FIELDTERMINATOR = ',',
        STRING_DELIMITER = '"',
        HEADER_ROW = TRUE
    )
     WITH(
        CountryID TINYINT,
        CountryName VARCHAR(50),
        FormalName VARCHAR(100),
        IsoAlpha3Code VARCHAR(3),
        IsoNumericCode SMALLINT,
        CountryType VARCHAR(25),
        LatestRecordedPopulation INT,
        Continent VARCHAR(25),
        Region VARCHAR(25),
        Subregion VARCHAR(30),
        LastEditedBy SMALLINT,
        ValidFrom DATETIME2,
        ValidTo DATETIME2
    ) AS rows;
GO

--IncrementalStateProvinces
CREATE VIEW LDW.vwIncrementalStateProvinces AS
    SELECT *,
           CAST(rows.filepath(1) AS DATE) AS FilePathDate
    FROM OPENROWSET(
        BULK 'Changed_Dims/*/Application_StateProvinces/*.txt',
        DATA_SOURCE = 'SourceDataLake',
        PARSER_VERSION = '2.0',
        FORMAT = 'CSV',
        FIELDTERMINATOR = ',',
        STRING_DELIMITER = '"',
        HEADER_ROW = TRUE
    )
    WITH(
        StateProvinceID TINYINT,
        StateProvinceCode VARCHAR(2),
        StateProvinceName VARCHAR(30),
        CountryID TINYINT,
        SalesTerritory VARCHAR(25),
        LatestRecordedPopulation INT,
        LastEditedBy SMALLINT,
        ValidFrom DATETIME2,
        ValidTo DATETIME2
    ) AS rows;
GO

-- || CREATE VIEWS TO COMBINE RAW HISTORICAL AND RAW INCREMENTAL DIM DATA ||

-- LatestCustomers
CREATE VIEW LDW.vwLatestCustomers AS
WITH CombinedCustomers AS (
    SELECT CustomerID,
           CustomerName,
           BillToCustomerID,
           CustomerCategoryID,
           BuyingGroupID,
           PrimaryContactPersonID,
           AlternateContactPersonID,
           DeliveryMethodID,
           DeliveryCityID,
           PostalCityID,
           CreditLimit,
           AccountOpenedDate,
           StandardDiscountPercentage,
           IsStatementSent,
           IsOnCreditHold,
           PaymentDays,
           PhoneNumber,
           FaxNumber,
           DeliveryRun,
           RunPosition,
           WebsiteURL,
           DeliveryAddressLine1,
           DeliveryAddressLine2,
           DeliveryPostalCode,
           DeliveryLocation,
           PostalAddressLine1,
           PostalAddressLine2,
           PostalPostalCode,
           LastEditedBy,
           ValidFrom,
           ValidTo
    FROM LDW.vwCustomers

    UNION ALL

    SELECT CustomerID,
           CustomerName,
           BillToCustomerID,
           CustomerCategoryID,
           BuyingGroupID,
           PrimaryContactPersonID,
           AlternateContactPersonID,
           DeliveryMethodID,
           DeliveryCityID,
           PostalCityID,
           CreditLimit,
           AccountOpenedDate,
           StandardDiscountPercentage,
           IsStatementSent,
           IsOnCreditHold,
           PaymentDays,
           PhoneNumber,
           FaxNumber,
           DeliveryRun,
           RunPosition,
           WebsiteURL,
           DeliveryAddressLine1,
           DeliveryAddressLine2,
           DeliveryPostalCode,
           DeliveryLocation,
           PostalAddressLine1,
           PostalAddressLine2,
           PostalPostalCode,
           LastEditedBy,
           ValidFrom,
           ValidTo
    FROM LDW.vwIncrementalCustomers
)
SELECT *
FROM (
    SELECT *,
           ROW_NUMBER() OVER(PARTITION BY CustomerID ORDER BY ValidFrom DESC) AS rn
    FROM CombinedCustomers
) AS sub
WHERE rn = 1;
GO

-- LatestSuppliers
CREATE VIEW LDW.vwLatestSuppliers AS
WITH CombinedSuppliers AS (
    SELECT SupplierID,
           SupplierName,
           SupplierCategoryID,
           PrimaryContactPerson,
           AlternateContactPersonID,
           DeliveryMethodID,
           DeliveryCityID,
           PostalCityID,
           SupplierReference,
           BankAccountName,
           BankAccountBranch,
           BankAccountCode,
           BankAccountNumber,
           BankInternationalCode,
           PaymentDays,
           InternalComments,
           PhoneNumber,
           FaxNumber,
           WebsiteURL,
           DeliveryAddressLine1,
           DeliveryAddressLine2,
           DeliveryPostalCode,
           DeliveryLocation,
           PostalAddressLine1,
           PostalAddressLine2,
           PostalPostalCode,
           LastEditedBy,
           ValidFrom,
           ValidTo
    FROM LDW.vwSuppliers

    UNION ALL

    SELECT SupplierID,
           SupplierName,
           SupplierCategoryID,
           PrimaryContactPerson,
           AlternateContactPersonID,
           DeliveryMethodID,
           DeliveryCityID,
           PostalCityID,
           SupplierReference,
           BankAccountName,
           BankAccountBranch,
           BankAccountCode,
           BankAccountNumber,
           BankInternationalCode,
           PaymentDays,
           InternalComments,
           PhoneNumber,
           FaxNumber,
           WebsiteURL,
           DeliveryAddressLine1,
           DeliveryAddressLine2,
           DeliveryPostalCode,
           DeliveryLocation,
           PostalAddressLine1,
           PostalAddressLine2,
           PostalPostalCode,
           LastEditedBy,
           ValidFrom,
           ValidTo
    FROM LDW.vwIncrementalSuppliers
)
SELECT *
FROM (
    SELECT *,
           ROW_NUMBER() OVER(PARTITION BY SupplierID ORDER BY ValidFrom DESC) AS rn
    FROM CombinedSuppliers
) AS sub
WHERE rn = 1;
GO

-- LatestCities
CREATE VIEW LDW.vwLatestCities AS
WITH CombinedCities AS (
    SELECT CityID,
           CityName,
           StateProvinceID,
           Location,
           LatestRecordedPopulation,
           LastEditedBy,
           ValidFrom,
           ValidTo
    FROM LDW.vwCities

    UNION ALL

    SELECT CityID,
           CityName,
           StateProvinceID,
           Location,
           LatestRecordedPopulation,
           LastEditedBy,
           ValidFrom,
           ValidTo
    FROM LDW.vwIncrementalCities
)
SELECT *
FROM (
    SELECT *,
           ROW_NUMBER() OVER(PARTITION BY CityID ORDER BY ValidFrom DESC) AS rn
    FROM CombinedCities
) AS sub
WHERE rn = 1;
GO

-- LatestCountries
CREATE VIEW LDW.vwLatestCountries AS
WITH CombinedCountries AS (
    SELECT CountryID,
           CountryName,
           FormalName,
           IsoAlpha3Code,
           IsoNumericCode,
           CountryType,
           LatestRecordedPopulation,
           Continent,
           Region,
           Subregion,
           LastEditedBy,
           ValidFrom,
           ValidTo  
    FROM LDW.vwCountries -- initial data

    UNION ALL

    SELECT CountryID,
           CountryName,
           FormalName,
           IsoAlpha3Code,
           IsoNumericCode,
           CountryType,
           LatestRecordedPopulation,
           Continent,
           Region,
           Subregion,
           LastEditedBy,
           ValidFrom,
           ValidTo  
    FROM LDW.vwIncrementalCountries -- incremental data
)
SELECT *
FROM (
    SELECT *,
           ROW_NUMBER() OVER(PARTITION BY CountryID ORDER BY ValidFrom DESC) AS rn
    FROM CombinedCountries
) AS sub
WHERE rn = 1;
GO

-- LatestStateProvinces
CREATE VIEW LDW.vwLatestStateProvinces AS
WITH CombinedStateProvinces AS (
    SELECT StateProvinceID,
           StateProvinceCode,
           StateProvinceName,
           CountryID,
           SalesTerritory,
           LatestRecordedPopulation,
           LastEditedBy,
           ValidFrom,
           ValidTo
    FROM LDW.vwStateProvinces

    UNION ALL

    SELECT StateProvinceID,
           StateProvinceCode,
           StateProvinceName,
           CountryID,
           SalesTerritory,
           LatestRecordedPopulation,
           LastEditedBy,
           ValidFrom,
           ValidTo
    FROM LDW.vwIncrementalStateProvinces
)
SELECT *
FROM (
    SELECT *,
           ROW_NUMBER() OVER(PARTITION BY StateProvinceID ORDER BY ValidFrom DESC) AS rn
    FROM CombinedStateProvinces
) AS sub
WHERE rn = 1;
GO

-- || CREATE COMPLETE SCD TYPE 2 LOGIC FOR CUSTOMER & SUPPLIER DIMS ||

-- DimCustomersSCD
CREATE VIEW LDW.vwDimCustomersSCD AS
SELECT CustomerKey,
       CustomerID,
       CustomerName,
       CustomerCategoryName,
       BuyingGroupName,
       DeliveryMethodName,
       CreditLimit,
       StandardDiscountPercentage,
       CityName,
       StateProvinceName,
       SalesTerritory,
       CountryName,
       Continent,
       Region,
       Subregion,
       ValidFromDate,
       ISNULL(DATEADD(DAY, -1, LEAD(ValidFromDate) OVER(PARTITION BY CustomerID ORDER BY CustomerKey)), '9999-12-31') AS ValidToDate,
       IIF(ROW_NUMBER() OVER(PARTITION BY CustomerID ORDER BY CustomerKey DESC) = 1, 'Y', 'F') AS IsActiveMember
FROM LDW.vwDimCustomers;
GO

-- DimSuppliersSCD
CREATE VIEW LDW.vwDimSuppliersSCD AS
SELECT SupplierKey,
       SupplierID,
       SupplierName,
       SupplierCategoryName,
       PaymentDays,
       DeliveryMethodName,
       CityName,
       PhoneNumber,
       BankAccountName,
       ValidFromDate,
       ISNULL(DATEADD(DAY, -1, LEAD(ValidFromDate) OVER(PARTITION BY SupplierID ORDER BY SupplierKey)), '9999-12-31') AS ValidToDate,
       IIF(ROW_NUMBER() OVER(PARTITION BY SupplierID ORDER BY SupplierKey DESC) = 1, 'Y', 'N') AS IsActiveMember
FROM LDW.vwDimSuppliers;
GO


