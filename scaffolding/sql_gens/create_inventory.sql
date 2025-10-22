CREATE TABLE dbo.Inventory (
    [ingredient] NVARCHAR(255),
    [supplier] NVARCHAR(255),
    [cost_per_unit] DECIMAL(10,2),
    [stock_level] INT,
    [last_updated] NVARCHAR(255),
    [ingestion_timestamp] NVARCHAR(255)
);