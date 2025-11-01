-- Stored procedure for writing incremental supplier data
CREATE OR ALTER PROCEDURE STG.uspIncrementalSuppliersLoad @ProcessDate DATE
WITH ENCRYPTION AS
BEGIN

IF OBJECT_ID('STG.DimSuppliers') IS NOT NULL
    DROP EXTERNAL TABLE STG.DimSuppliers

DECLARE @MaxKey SMALLINT
SELECT @MaxKey = MAX(SupplierKey) FROM LDW.vwDimSuppliers
DECLARE @Location VARCHAR(100) = CONCAT('Conformed/Dimensions/Dim_Suppliers/',
                                         FORMAT(@ProcessDate, 'yyyy-MM-dd'))

DECLARE @CreateExternalTable NVARCHAR(2000) = 
'CREATE EXTERNAL TABLE STG.DimSuppliers
    WITH (
        LOCATION = ''' + @Location + ''',
        DATA_SOURCE = SourceDataLake,
        FILE_FORMAT = ParquetFormat
    )
    AS
    SELECT CAST(ROW_NUMBER() OVER(ORDER BY SU.SupplierID) + ' + CAST(@MaxKey AS TINYINT) + ' AS TINYINT) AS SupplierKey,
           SU.SupplierID,
           SU.SupplierName,
           SC.SupplierCategoryName,
           SU.PaymentDays,
           D.DeliveryMethodName,
           CI.CityName,
           SU.PhoneNumber,
           SU.BankAccountName,
           CAST(SU.ValidFrom AS DATE) AS ValidFromDate
    FROM LDW.vwIncrementalSuppliers SU
    LEFT JOIN LDW.vwSupplierCategories SC
        ON SU.SupplierCategoryID = SC.SupplierCategoryID
    LEFT JOIN LDW.vwDeliveryMethods D
        ON SU.DeliveryMethodID = D.DeliveryMethodID
    LEFT JOIN LDW.vwLatestCities CI
        ON SU.DeliveryCityID = CI.CityID
    WHERE SU.FilePathDate = ''' + CAST(@ProcessDate AS CHAR(10)) + '''
    ORDER BY SU.SupplierID'

EXEC sp_executesql @CreateExternalTable

END;