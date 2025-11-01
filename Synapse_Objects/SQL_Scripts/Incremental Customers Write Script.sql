-- Stored procedure for writing incremental customer data
CREATE OR ALTER PROCEDURE STG.uspIncrementalCustomersLoad @ProcessDate DATE
WITH ENCRYPTION AS
BEGIN

IF OBJECT_ID('STG.DimCustomers') IS NOT NULL
    DROP EXTERNAL TABLE STG.DimCustomers

DECLARE @MaxKey SMALLINT
SELECT @MaxKey = MAX(CustomerKey) FROM LDW.vwDimCustomers
DECLARE @Location VARCHAR(100) = CONCAT('Conformed/Dimensions/Dim_Customers/',
                                         FORMAT(@ProcessDate, 'yyyy-MM-dd'))

DECLARE @CreateExternalTable NVARCHAR(2000) = 
'CREATE EXTERNAL TABLE STG.DimCustomers
    WITH (
        LOCATION = ''' + @Location + ''',
        DATA_SOURCE = SourceDataLake,
        FILE_FORMAT = ParquetFormat
    )
    AS
    SELECT CAST(ROW_NUMBER() OVER(ORDER BY C.CustomerID) + ' + CAST(@MaxKey AS VARCHAR(5)) + ' AS SMALLINT) AS CustomerKey,
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
    FROM LDW.vwIncrementalCustomers C
    LEFT JOIN LDW.vwCustomerCategories CC
        ON C.CustomerCategoryID = CC.CustomerCategoryID
    LEFT JOIN LDW.vwBuyingGroups BG
        ON C.BuyingGroupID = BG.BuyingGroupID
    LEFT JOIN LDW.vwDeliveryMethods D
        ON C.DeliveryMethodID = D.DeliveryMethodID
    LEFT JOIN LDW.vwLatestCities CI
        ON C.DeliveryCityID = CI.CityID
    LEFT JOIN LDW.vwLatestStateProvinces S
        ON CI.StateProvinceID = S.StateProvinceID
    LEFT JOIN LDW.vwLatestCountries CO
        ON S.CountryID = CO.CountryID
    WHERE C.FilePathDate = ''' + CAST(@ProcessDate AS CHAR(10)) + '''
    ORDER BY C.CustomerID'

EXEC sp_executesql @CreateExternalTable

END;