SET ANSI_NULLS ON
GO
SET QUOTED_IDENTIFIER ON
GO
CREATE PROC [test].[USP_MS_GET_PART_OBJ_PART_BOUNDARIES] @TABLENAME [VARCHAR](200) AS
/*
Author - Rakesh Sharma
Description - Return the boundaries for the partition
*/
SET NOCOUNT ON
BEGIN
SELECT      s.[name]                        AS      [Schema_Name]
,           t.[name]                        AS      [Table_Name]
,           i.[name]                        AS      [Index_Name]
,           p.[partition_number]            AS      [Partition_Number]
--,           p.[rows]                        AS      [partition_row_count]
,           '>= ' + cast(prv_left.[value] as varchar(15)   )                   AS      [Partition_Boundary_Value_Lower]
,           '< '+ cast(prv_right.[value]  as varchar(15))                    AS      [Partition_Boundary_Value_Upper]

,           p.[data_compression_desc]       AS      [Partition_Compression_Desc]
FROM        sys.schemas s
JOIN        sys.tables t                    ON      t.[schema_id]         = s.[schema_id]
JOIN        sys.partitions p                ON      p.[object_id]         = t.[object_id]
JOIN        sys.indexes i                   ON      i.[object_id]         = p.[object_id]
                                            AND     i.[index_id]          = p.[index_id]
JOIN        sys.data_spaces ds              ON      ds.[data_space_id]    = i.[data_space_id]
LEFT JOIN   sys.partition_schemes ps        ON      ps.[data_space_id]    = ds.[data_space_id]
LEFT JOIN   sys.partition_functions pf      ON      pf.[function_id]      = ps.[function_id]

LEFT OUTER JOIN sys.partition_range_values prv_right
ON prv_right.function_id = ps.function_id
AND prv_right.boundary_id = p.partition_number
LEFT OUTER JOIN sys.partition_range_values prv_left
ON prv_left.function_id = ps.function_id
AND prv_left.boundary_id = p.partition_number - 1
WHERE       p.[index_id] <=1
AND
T.name=@TABLENAME
order by  cast(prv_left.[value] as int)
END
GO


SET ANSI_NULLS ON
GO
SET QUOTED_IDENTIFIER ON
GO
CREATE PROC [test].[USP_MS_GET_PART_DIST_NODE_LEVEL_TOTAL_ROW_COUNT_WITH_PART] @TABLENAME [VARCHAR](200),@schema [varchar](50) AS
/*
Author - Rakesh Sharma
Description - Return the boundaries for the partition along with Row Count
*/

SET NOCOUNT ON
BEGIN

SELECT t.name,pnp.partition_number,SUM(nps.[row_count])RowsInAllDist
INTO #TEMPROWINFO
 FROM
   sys.tables t
INNER JOIN sys.indexes i
    ON  t.[object_id] = i.[object_id]
    AND i.[index_id] <= 1 /* HEAP = 0, CLUSTERED or CLUSTERED_COLUMNSTORE =1 */
INNER JOIN sys.pdw_table_mappings tm
    ON t.[object_id] = tm.[object_id]
INNER JOIN sys.pdw_nodes_tables nt
    ON tm.[physical_name] = nt.[name]
INNER JOIN sys.pdw_nodes_partitions pnp 
    ON nt.[object_id]=pnp.[object_id] 
    AND nt.[pdw_node_id]=pnp.[pdw_node_id] 
    AND nt.[distribution_id] = pnp.[distribution_id]
INNER JOIN sys.dm_pdw_nodes_db_partition_stats nps
    ON nt.[object_id] = nps.[object_id]
    AND nt.[pdw_node_id] = nps.[pdw_node_id]
    AND nt.[distribution_id] = nps.[distribution_id]
    AND pnp.[partition_id]=nps.[partition_id]
WHERE t.name=@TABLENAME
GROUP BY t.name,pnp.partition_number

SELECT      s.[name]                        AS      [Schema_Name]
,           t.[name]                        AS      [Table_Name]
,           i.[name]                        AS      [Index_Name]
,           p.[partition_number]            AS      [Partition_Number]
--,           p.[rows]                        AS      [partition_row_count]
,           '>= ' + cast(prv_left.[value] as varchar(15)   )                   AS      [Partition_Boundary_Value_Lower]
,           '< '+ cast(prv_right.[value]  as varchar(15))                    AS      [Partition_Boundary_Value_Upper]

,           p.[data_compression_desc]       AS      [Partition_Compression_Desc]
INTO #TEMPPARTINFO
FROM        sys.schemas s
JOIN        sys.tables t                    ON      t.[schema_id]         = s.[schema_id]
JOIN        sys.partitions p                ON      p.[object_id]         = t.[object_id]
JOIN        sys.indexes i                   ON      i.[object_id]         = p.[object_id]
                                            AND     i.[index_id]          = p.[index_id]
JOIN        sys.data_spaces ds              ON      ds.[data_space_id]    = i.[data_space_id]
LEFT JOIN   sys.partition_schemes ps        ON      ps.[data_space_id]    = ds.[data_space_id]
LEFT JOIN   sys.partition_functions pf      ON      pf.[function_id]      = ps.[function_id]

LEFT OUTER JOIN sys.partition_range_values prv_right
ON prv_right.function_id = ps.function_id
AND prv_right.boundary_id = p.partition_number
LEFT OUTER JOIN sys.partition_range_values prv_left
ON prv_left.function_id = ps.function_id
AND prv_left.boundary_id = p.partition_number - 1
WHERE       p.[index_id] <=1
AND
T.name=@TABLENAME
and
s.name =@schema

SELECT B.Schema_Name, A.NAME AS TABLENAME,B.INDEX_NAME,A.[Partition_Number],B.[Partition_Boundary_Value_Lower],
B.[Partition_Boundary_Value_Upper],A.RowsInAllDist
 FROM #TEMPROWINFO A
JOIN #TEMPPARTINFO B
ON
A.NAME=B.[Table_Name]
AND A.[Partition_Number]=B.[Partition_Number]
ORDER BY A.[Partition_Number]
DROP TABLE #TEMPROWINFO
DROP TABLE #TEMPPARTINFO
END
GO


SET ANSI_NULLS ON
GO
SET QUOTED_IDENTIFIER ON
GO
CREATE PROC [test].[USP_MS_GET_PART_DIST_NODE_LEVEL] @TABLENAME [VARCHAR](200) AS

/*
Author - Rakesh Sharma
Description - Return number of rows in each partition in all 60 distributions
*/

SET NOCOUNT ON
BEGIN
SELECT t.name,nt.distribution_id as DistributionID,pnp.partition_number as Partition_Number,nps.[row_count],nps.[used_page_count]*8.0/1024 as usedSpaceMB
 FROM
   sys.tables t
INNER JOIN sys.indexes i
    ON  t.[object_id] = i.[object_id]
    AND i.[index_id] <= 1 /* HEAP = 0, CLUSTERED or CLUSTERED_COLUMNSTORE =1 */
INNER JOIN sys.pdw_table_mappings tm
    ON t.[object_id] = tm.[object_id]
INNER JOIN sys.pdw_nodes_tables nt
    ON tm.[physical_name] = nt.[name]
INNER JOIN sys.pdw_nodes_partitions pnp 
    ON nt.[object_id]=pnp.[object_id] 
    AND nt.[pdw_node_id]=pnp.[pdw_node_id] 
    AND nt.[distribution_id] = pnp.[distribution_id]
INNER JOIN sys.dm_pdw_nodes_db_partition_stats nps
    ON nt.[object_id] = nps.[object_id]
    AND nt.[pdw_node_id] = nps.[pdw_node_id]
    AND nt.[distribution_id] = nps.[distribution_id]
    AND pnp.[partition_id]=nps.[partition_id]
WHERE t.name=@TABLENAME
ORDER BY nt.distribution_id asc
END
GO
