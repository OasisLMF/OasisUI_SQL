USE [Flamingo_dev]
GO
/****** Object:  StoredProcedure [dbo].[generateOasisFilesRecords]    Script Date: 8/7/2019 4:00:29 PM ******/
DROP PROCEDURE [dbo].[generateOasisFilesRecords]
GO
/****** Object:  StoredProcedure [dbo].[generateOasisFiles2]    Script Date: 8/7/2019 4:00:29 PM ******/
DROP PROCEDURE [dbo].[generateOasisFiles2]
GO
/****** Object:  StoredProcedure [dbo].[dropFlamingoTables]    Script Date: 8/7/2019 4:00:29 PM ******/
DROP PROCEDURE [dbo].[dropFlamingoTables]
GO
/****** Object:  StoredProcedure [dbo].[CreateTempFlamingoTables]    Script Date: 8/7/2019 4:00:29 PM ******/
DROP PROCEDURE [dbo].[CreateTempFlamingoTables]
GO
/****** Object:  StoredProcedure [dbo].[CreateTempFlamingoTables]    Script Date: 8/7/2019 4:00:29 PM ******/
SET ANSI_NULLS ON
GO
SET QUOTED_IDENTIFIER ON
GO

CREATE PROCEDURE [dbo].[CreateTempFlamingoTables] 
	@GUID_CLEAN varchar(max) output
AS
BEGIN
    --This procedure creates the temporary Flamingo Tables.
	--These have the suffix of GUID_CLEAN
	--GUID_CLEAN is assigned inside the procedure and returned .
	--The tables for this procedure can be cleaned up using 
	--DropTempFlamingoTables procedure
	--Example  of use:
	----
	--declare @GUID_CLEAN varchar(max);

	--exec CreateTempFlamingoTables
	--@GUID_CLEAN = @GUID_CLEAN output;
	
	--print  @GUID_CLEAN
	SET NOCOUNT ON;

	--DECLARE @GUID_Value UNIQUEIDENTIFIER = NEWID()
	DECLARE @tablename varchar(max)
	DECLARE @sql nvarchar(max)

	--set @GUID_CLEAN  = replace(cast(@GUID_Value as varchar(max)), '-', '')
	--Replaces '-' in GUID with empty string
	--@GUID_CLEAN varchar(max) = convert(varchar(max),@GUID_Value);
	--print @GUID_CLEAN


	select @tablename = 'FlamingoCOVERAGES_'+@GUID_CLEAN
	set @sql=
	'create table '+@tablename+'
		(coverage_id int NOT NULL, 
		tiv float NOT NULL)'
	exec sp_executesql @sql

	select @tablename = 'FlamingoFMDICT_'+@GUID_CLEAN
	set @sql=
	'create table '+@tablename+'
		(output_id int NOT NULL, 
		item_id int NOT NULL , 
		agg_id int NOT NULL, 
		layer_id int NOT NULL, 
		policy_name nvarchar(255) null, 
		layer_name nvarchar(255) null)'
	exec sp_executesql @sql


	--FMPOLICY
	select @tablename = 'FlamingoFMPOLICYTC_'+@GUID_CLEAN
	set @sql=
	'create table '+@tablename+'
		(layer_id int NOT NULL, 
		level_id int NOT NULL, 	
		agg_id int NOT NULL, 
		policytc_id int NOT NULL)'
	exec sp_executesql @sql


	--FMPROFILE
	select @tablename = 'FlamingoFMPROFILE_'+@GUID_CLEAN
	set @sql = 
	'create table '+@tablename+'
		(profile_id int NOT NULL,
		calcrule_id int NOT NULL,
		deductible1 float NOT NULL,
		deductible2 float NOT NULL,
		deductible3 float NOT NULL,
		attachment1 float NOT NULL,
		limit1 float NOT NULL,
		share1 float NOT NULL,
		share2 float NOT NULL,
		share3 float NOT NULL)'
	exec sp_executesql @sql

	--FMPROGRAMME
	select @tablename = 'FlamingoFMPROGRAMME_'+@GUID_CLEAN
	set @sql = 
	'create table '+@tablename+'
		(from_agg_id int NOT NULL,
		level_id int NOT NULL,
		to_agg_id int NOT NULL)'
	exec sp_executesql @sql


	--FMXREF
	select @tablename = 'FlamingoFMXREF_'+@GUID_CLEAN
	set @sql = 
	'create table '+@tablename+'
		(output_id int NOT NULL,
		summary_id int NOT NULL,
		summaryset_id int NOT NULL)'
	exec sp_executesql @sql


	--FMXREFALLOC
	select @tablename = 'FlamingoFMXREFALLOC_'+@GUID_CLEAN
	set @sql = 
	'create table '+@tablename+'
		(output_id int NOT NULL,
		agg_id int NOT NULL,
		layer_id int NOT NULL)'
	exec sp_executesql @sql

	--FMSUMMARYXREF
	select @tablename = 'FlamingoFMSUMMARYXREF_'+@GUID_CLEAN
	set @sql =
	'create table '+@tablename+'
		(output int NOT NULL,
		summary_id int NOT NULL,
		summaryset_id int NOT NULL)'
	exec sp_executesql @sql

	--GULSMARYXREF
	select @tablename = 'FlamingoGULSUMMARYXREF_'+@GUID_CLEAN
	set @sql =
	'create table '+@tablename+'
		(coverage_id int NOT NULL,
		summary_id int NOT NULL,
		summaryset_id int NOT NULL)'
	exec sp_executesql @sql


	--FMSUMMARYXREF
	select @tablename = 'FlamingoITEMDICT_'+@GUID_CLEAN
	set @sql =
	'create table '+@tablename+'
		(item_id int NOT NULL,
		coverage_id int NOT NULL,
		location_id int NULL,
		location_desc nvarchar(255) NULL,
		lob_id int NULL,
		lob_desc nvarchar(255) NULL,
		county_id int NULL,
		county_desc nvarchar(255) NULL,
		state_id int NULL,
		state_desc nvarchar(255) NULL,
		portfolionumber_id int NULL,
		portfolionumber_desc nvarchar(50) NULL,
		locationgroup_id int NULL,
		locationgroup_desc nvarchar(50) NULL)'
	exec sp_executesql @sql

	--ITEMS
	select @tablename = 'FlamingoITEMS_'+@GUID_CLEAN
	set @sql = 
	'create table '+@tablename+'
		(item_id int NOT NULL,
		coverage_id int NOT NULL,
		areaperil_id int NOT NULL,
		vulnerability_id int NOT NULL,
		group_id int NOT NULL)'
	exec sp_executesql @sql


	--RISUMMARYXRFF
	select @tablename = 'FlamingoRISUMMARYXREF_'+@GUID_CLEAN
	set @sql = 
	'create table  '+@tablename+'
		(output_id int NOT NULL,
		summary_id int NOT NULL,
		summaryset_id int NOT NULL)'
	exec sp_executesql @sql

end;



GO
/****** Object:  StoredProcedure [dbo].[dropFlamingoTables]    Script Date: 8/7/2019 4:00:29 PM ******/
SET ANSI_NULLS ON
GO
SET QUOTED_IDENTIFIER ON
GO

CREATE procedure [dbo].[dropFlamingoTables] @GUID_CLEAN nvarchar(max)
as
begin;



declare @tablename varchar(max) 
declare @sql nvarchar(max)

set @tablename = 'FlamingoFMPROFILE_'+@GUID_CLEAN
set @sql = 'drop table ['+@tablename+'] '
exec sp_executesql @sql 

set @tablename = 'FlamingoCOVERAGES_'+@GUID_CLEAN
set @sql = 'drop table ['+@tablename+'] '
exec sp_executesql @sql

set @tablename = 'FlamingoFMDICT_'+@GUID_CLEAN
set @sql = 'drop table ['+@tablename+'] '
exec sp_executesql @sql

set @tablename = 'FlamingoFMPOLICYTC_'+@GUID_CLEAN
set @sql = 'drop table ['+@tablename+'] '
exec sp_executesql @sql

set @tablename = 'FlamingoFMPROGRAMME_'+@GUID_CLEAN
set @sql = 'drop table ['+@tablename+'] '
exec sp_executesql @sql

set @tablename = 'FlamingoFMXREF_'+@GUID_CLEAN
set @sql = 'drop table ['+@tablename+'] '
exec sp_executesql @sql

set @tablename = 'FlamingoFMSUMMARYXREF_'+@GUID_CLEAN
set @sql = 'drop table ['+@tablename+'] '
exec sp_executesql @sql

set @tablename = 'FlamingoGULSUMMARYXREF_'+@GUID_CLEAN
set @sql = 'drop table ['+@tablename+'] '
exec sp_executesql @sql

set @tablename = 'FlamingoITEMDICT_'+@GUID_CLEAN
set @sql = 'drop table ['+@tablename+'] '
exec sp_executesql @sql

set @tablename = 'FlamingoITEMS_'+@GUID_CLEAN
set @sql = 'drop table ['+@tablename+'] '
exec sp_executesql @sql

set @tablename = 'FlamingoRISUMMARYXREF_'+@GUID_CLEAN
set @sql = 'drop table ['+@tablename+'] '
exec sp_executesql @sql


set @tablename = 'FlamingoFMXREFALLOC_'+@GUID_CLEAN
set @sql = 'drop table ['+@tablename+'] '
exec sp_executesql @sql

end;

GO
/****** Object:  StoredProcedure [dbo].[generateOasisFiles2]    Script Date: 8/7/2019 4:00:29 PM ******/
SET ANSI_NULLS ON
GO
SET QUOTED_IDENTIFIER ON
GO

CREATE procedure [dbo].[generateOasisFiles2] @ProgOasisId int
AS
SET ANSI_NULLS ON;
SET ANSI_WARNINGS ON;
SET NOCOUNT ON;
----------------------------------------------------------------------------
--log database usage
declare	@ProcedureName nvarchar(255)  = (Select OBJECT_NAME(@@PROCID))
declare	@ParameterList nvarchar(2500) = '@ProgOasisId = ' + convert(nvarchar,@ProgOasisId)
declare	@LogTimestamp datetime = getdate()
exec updateLogDatabaseUsage @ProcedureName,@ParameterList,@LogTimestamp
----------------------------------------------------------------------------

--declare @ProgOasisId int = 1
--Declare Variables
Declare @ProgId int = (Select ProgID From ProgOasis Where ProgOasisId = @ProgOasisID)
Declare @ModelId int = (Select ModelID From ProgOasis Where ProgOasisId = @ProgOasisID)
Declare @TransformID int = (Select TransformID From Prog Where ProgID = @ProgId)
Declare @CanLocProfileID int = (Select ProfileID From ProfileResource AS PR Join [Resource] AS R on PR.ResourceID = R.ResourceID Where R.ResourceTable = 'Transform' And ResourceKey = @TransformID And ResourceTypeID = 118)
Declare @CanAccProfileID int = (Select ProfileID From ProfileResource AS PR Join [Resource] AS R on PR.ResourceID = R.ResourceID Where R.ResourceTable = 'Transform' And ResourceKey = @TransformID And ResourceTypeID = 119)
Declare @TIVFieldID int = 2
Declare @LocFieldID int = 3
Declare @StateCodeID int = 29
Declare @CountyCode int = 30
Declare @PostCode int = 7
Declare @SourceLocNumber int = 10
Declare @OccScheme int = 13
Declare @OccCode int = 14
Declare @LineOfBusinessId  int = 61
Declare @AccountNumber int = 6
Declare @SubLimitRef int = 54
Declare @ModelGroupField int = 300
Declare @SQL nvarchar(max)
Declare @ProfileElementID int
Declare @Level int
Declare @MaxLevel int
Declare @PortfolioNumberId int = 62
Declare @LocationGroup int = 63
Declare @CorrelationGroup int = 64






insert into LogDatabaseUsage values ('generateOasisFiles2','started',getdate(),null)

--update status
update progoasis set [status] = 'Generating Oasis Files' where progoasisid = @ProgOasisId
--Item Staging File
Create Table #Item (ItemId int identity(1,1) primary key, InterestGroupId int, InterestRiskID int, InterestSubRiskID int, InterestExposureID int,
					GroupId int NULL, LocId int NULL, PerilId nvarchar(255) NULL, CoverageTypeId nvarchar(255) NULL, TIV float NULL, 
					AreaPerilId bigint NULL, VulnerabilityId int NULL, PolicyID int NULL, AltItemID int NULL, CoverageId int NULL, StateCode nvarchar(255) NULL,
					CountyCode nvarchar(255) NULL, PostCode nvarchar(255) NULL, SourceLocNumber nvarchar(255) NULL, OccScheme nvarchar(255) NULL, 
					OccCode nvarchar(255) NULL, LineOfBusiness nvarchar(255) NULL, AccountNumber nvarchar(255) NULL, SubLimitRef nvarchar(255) NULL, ElementDimensionID int NULL, ProgID int NULL,
					IsValid bit null, PortfolioNumber nvarchar(255) null, LocationGroup nvarchar(255) null, CorrelationGroup int null)
CREATE INDEX Idx_Item_InterestRiskID	 ON #Item (InterestRiskID)
CREATE INDEX Idx_Item_InterestSubRiskID  ON #Item (InterestSubRiskID)
CREATE INDEX Idx_Item_InterestExposureID ON #Item (InterestExposureID)
Insert Into	#Item (InterestGroupId, InterestRiskID, InterestSubRiskID, InterestExposureID,GroupId, LocId, PerilId, CoverageTypeId, TIV, 
					AreaPerilId, VulnerabilityId, PolicyID, AltItemID, CoverageId, StateCode, CountyCode, PostCode, SourceLocNumber, 
					OccScheme, OccCode, LineOfBusiness, AccountNumber,SubLimitRef,ElementDimensionID,ProgID,IsValid,PortfolioNumber,LocationGroup,
					CorrelationGroup)
Select	IG.InterestGroupId,
		IR.InterestRiskID,
		ISR.InterestSubRiskID,
		IE.InterestExposureID,
		NULL AS GroupId,
		NULL AS LocId,
		PVD.PerilID,
		PVD.CoverageTypeId,
		IEV.FieldValue AS TIV,
		NULL AS AreaPerilId,
		NULL AS VulnerabilityId,
		NULL AS PolicyID,
		NULL AS AltItemID,
		NULL AS CoverageId,
		cast(NULL as nvarchar(255)) AS StateCode,
		cast(NULL as nvarchar(255)) AS CountyCode,
		cast(NULL as nvarchar(255)) AS PostCode,
		cast(NULL as nvarchar(255)) AS SourceLocNumber,
		cast(NULL as nvarchar(255)) AS OccScheme,
		cast(NULL as nvarchar(255)) AS OccCode,
		cast(NULL as nvarchar(255)) AS LineOfBusiness,
		cast(NULL as nvarchar(255)) AS AccountNumber,
		cast(NULL as nvarchar(255)) AS SubLimitRef,
		PVD.ElementDimensionID,
		@ProgId AS ProgID,
		1 AS IsValid,
		cast(NULL as nvarchar(255)) AS PortfolioNumber,
		cast(NULL as nvarchar(255)) AS LocationGroup,
		cast(NULL as int) AS CorrelationGroup
From	InterestGroup AS IG
Join	InterestRisk AS IR on IG.InterestGroupID = IR.InterestGroupID
Join	InterestSubRisk AS ISR on IR.InterestRiskID = ISR.InterestRiskID
Join	InterestExposure AS IE on ISR.InterestSubRiskID = IE.InterestSubRiskID
Join	InterestExposureValues AS IEV on IE.InterestExposureID = IEV.InterestExposureID
Join	ProfileElement AS PE on IEV.ProfileElementID = PE.ProfileElementID
Join	ProfileValueDetail AS PVD on PE.ProfileElementID = PVD.ProfileElementID
Where	IG.ProgID = @ProgId
And		PE.FieldID = 2 --TIV
And		PE.ProfileID = @CanLocProfileID

insert into LogDatabaseUsage values ('generateOasisFiles2','created #item',getdate(),null)

--LocId
Set @ProfileElementID = (Select ProfileElementID From ProfileElement Where ProfileID = @CanLocProfileID and FieldID = @LocFieldID)
Select	@SQL = 'Update #Item Set LocID = FieldValue From	' + TableName + ' Where ProfileElementId = ' + convert(nvarchar,@ProfileElementID) + ' And #Item.' + Replace(TableName,'Values','ID') + ' = ' + TableName + '.' + Replace(TableName,'Values','ID') 
From	ProfileElement AS PE
Join	[Table] AS T on PE.TableID = T.TableID
Where	ProfileID = @CanLocProfileID 
And		PE.FieldID = @LocFieldID
exec sp_ExecuteSQL @SQL

--StateCode
Set @ProfileElementID = (Select ProfileElementID From ProfileElement Where ProfileID = @CanLocProfileID and FieldID = @StateCodeID)
if @ProfileElementID is not null
	begin
		Select	@SQL = 'Update #Item Set StateCode = FieldValue From	' + TableName + ' Where ProfileElementId = ' + convert(nvarchar,@ProfileElementID) + ' And #Item.' + Replace(TableName,'Values','ID') + ' = ' + TableName + '.' + Replace(TableName,'Values','ID') 
		From	ProfileElement AS PE
		Join	[Table] AS T on PE.TableID = T.TableID
		Where	ProfileID = @CanLocProfileID 
		And		PE.FieldID = @LocFieldID
		exec sp_ExecuteSQL @SQL
	end
else
	begin
		Update #Item Set StateCode = 'XX'
	end

--CountyCode
Set @ProfileElementID = (Select ProfileElementID From ProfileElement Where ProfileID = @CanLocProfileID and FieldID = @CountyCode)
if @ProfileElementID is not null
	begin
		Select	@SQL = 'Update #Item Set CountyCode = FieldValue From	' + TableName + ' Where ProfileElementId = ' + convert(nvarchar,@ProfileElementID) + ' And #Item.' + Replace(TableName,'Values','ID') + ' = ' + TableName + '.' + Replace(TableName,'Values','ID') 
		From	ProfileElement AS PE
		Join	[Table] AS T on PE.TableID = T.TableID
		Where	ProfileID = @CanLocProfileID 
		And		PE.FieldID = @LocFieldID
		exec sp_ExecuteSQL @SQL
	end
else
	begin
		Update #Item Set CountyCode = 'XX'
	end

--PostCode
Set @ProfileElementID = (Select ProfileElementID From ProfileElement Where ProfileID = @CanLocProfileID and FieldID = @PostCode)
if @ProfileElementID is not null
	begin
		Select	@SQL = 'Update #Item Set PostCode = FieldValue From	' + TableName + ' Where ProfileElementId = ' + convert(nvarchar,@ProfileElementID) + ' And #Item.' + Replace(TableName,'Values','ID') + ' = ' + TableName + '.' + Replace(TableName,'Values','ID') 
		From	ProfileElement AS PE
		Join	[Table] AS T on PE.TableID = T.TableID
		Where	ProfileID = @CanLocProfileID 
		And		PE.FieldID = @LocFieldID
		exec sp_ExecuteSQL @SQL
	end
else
	begin
		Update #Item Set PostCode = 'XXXXX'
	end

--SourceLocNumber
Set @ProfileElementID = (Select ProfileElementID From ProfileElement Where ProfileID = @CanLocProfileID and FieldID = @SourceLocNumber)
if @ProfileElementID is not null
	begin
		Select	@SQL = 'Update #Item Set SourceLocNumber = FieldValue From	' + TableName + ' Where ProfileElementId = ' + convert(nvarchar,@ProfileElementID) + ' And #Item.' + Replace(TableName,'Values','ID') + ' = ' + TableName + '.' + Replace(TableName,'Values','ID') 
		From	ProfileElement AS PE
		Join	[Table] AS T on PE.TableID = T.TableID
		Where	ProfileID = @CanLocProfileID 
		And		PE.FieldID = @LocFieldID
		exec sp_ExecuteSQL @SQL
	end
else
	begin
		Update #Item Set SourceLocNumber = 'XXX'
	end

--OccScheme
Set @ProfileElementID = (Select ProfileElementID From ProfileElement Where ProfileID = @CanLocProfileID and FieldID = @OccScheme)
if @ProfileElementID is not null
	begin
		Select	@SQL = 'Update #Item Set OccScheme = FieldValue From	' + TableName + ' Where ProfileElementId = ' + convert(nvarchar,@ProfileElementID) + ' And #Item.' + Replace(TableName,'Values','ID') + ' = ' + TableName + '.' + Replace(TableName,'Values','ID') 
		From	ProfileElement AS PE
		Join	[Table] AS T on PE.TableID = T.TableID
		Where	ProfileID = @CanLocProfileID 
		And		PE.FieldID = @LocFieldID
		exec sp_ExecuteSQL @SQL
	end
else
	begin
		Update #Item Set OccScheme = 'XXX'
	end
		

--OccCode
Set @ProfileElementID = (Select ProfileElementID From ProfileElement Where ProfileID = @CanLocProfileID and FieldID = @OccCode)
if @ProfileElementID is not null
	begin
		Select	@SQL = 'Update #Item Set OccCode = FieldValue From	' + TableName + ' Where ProfileElementId = ' + convert(nvarchar,@ProfileElementID) + ' And #Item.' + Replace(TableName,'Values','ID') + ' = ' + TableName + '.' + Replace(TableName,'Values','ID') 
		From	ProfileElement AS PE
		Join	[Table] AS T on PE.TableID = T.TableID
		Where	ProfileID = @CanLocProfileID 
		And		PE.FieldID = @LocFieldID
		exec sp_ExecuteSQL @SQL
	end
else
	begin
		Update #Item Set OccCode = 'XXX'
	end

--LineOfBusiness
Set @ProfileElementID = (Select ProfileElementID From ProfileElement Where ProfileID = @CanLocProfileID and FieldID = @LineOfBusinessId)
if @ProfileElementID is not null
	begin
		Select	@SQL = 'Update #Item Set LineOfBusiness = FieldValue From	' + TableName + ' Where ProfileElementId = ' + convert(nvarchar,@ProfileElementID) + ' And #Item.' + Replace(TableName,'Values','ID') + ' = ' + TableName + '.' + Replace(TableName,'Values','ID') 
		From	ProfileElement AS PE
		Join	[Table] AS T on PE.TableID = T.TableID
		Where	ProfileID = @CanLocProfileID 
		And		PE.FieldID = @LocFieldID
		exec sp_ExecuteSQL @SQL
	end
else
	begin
		Update #Item Set LineOfBusiness = 'XXX'
	end

--Portfolio Number
Set @ProfileElementID = (Select ProfileElementID From ProfileElement Where ProfileID = @CanLocProfileID and FieldID = @PortfolioNumberId)
if @ProfileElementID is not null
	begin
		Select	@SQL = 'Update #Item Set PortfolioNumber = FieldValue From	' + TableName + ' Where ProfileElementId = ' + convert(nvarchar,@ProfileElementID) + ' And #Item.' + Replace(TableName,'Values','ID') + ' = ' + TableName + '.' + Replace(TableName,'Values','ID') 
		From	ProfileElement AS PE
		Join	[Table] AS T on PE.TableID = T.TableID
		Where	ProfileID = @CanLocProfileID 
		And		PE.FieldID = @LocFieldID
		exec sp_ExecuteSQL @SQL
	end
else
	begin
		Update #Item Set PortfolioNumber = ''
	end

--LocationGroup
Set @ProfileElementID = (Select ProfileElementID From ProfileElement Where ProfileID = @CanLocProfileID and FieldID = @LocationGroup)
if @ProfileElementID is not null
	begin
		Select	@SQL = 'Update #Item Set LocationGroup = FieldValue From	' + TableName + ' Where ProfileElementId = ' + convert(nvarchar,@ProfileElementID) + ' And #Item.' + Replace(TableName,'Values','ID') + ' = ' + TableName + '.' + Replace(TableName,'Values','ID') 
		From	ProfileElement AS PE
		Join	[Table] AS T on PE.TableID = T.TableID
		Where	ProfileID = @CanLocProfileID 
		And		PE.FieldID = @LocFieldID
		exec sp_ExecuteSQL @SQL
	end
else
	begin
		Update #Item Set LocationGroup = ''
	end


--CorrelationGroup
Set @ProfileElementID = (Select ProfileElementID From ProfileElement Where ProfileID = @CanLocProfileID and FieldID = @CorrelationGroup)
if @ProfileElementID is not null
	begin
		Select	@SQL = 'Update #Item Set CorrelationGroup = FieldValue From	' + TableName + ' Where ProfileElementId = ' + convert(nvarchar,@ProfileElementID) + ' And #Item.' + Replace(TableName,'Values','ID') + ' = ' + TableName + '.' + Replace(TableName,'Values','ID') 
		From	ProfileElement AS PE
		Join	[Table] AS T on PE.TableID = T.TableID
		Where	ProfileID = @CanLocProfileID 
		And		PE.FieldID = @LocFieldID
		exec sp_ExecuteSQL @SQL
	end

--Oasis Keys
Update	#Item
Set		AreaPerilId = POK.AreaPerilId,
		VulnerabilityId = POK.VulnerabilityId
From	ProgOasisKeys AS POK
Where	#Item.LocId = POK.LocId
And		#Item.CoverageTypeId = POK.CoverageId
And		#Item.PerilId = POK.PerilId
And		POK.ProgOasisID = @ProgOasisID


--GroupID

Declare @GroupByField nvarchar(255)

Select	@GroupByField = ModelResourceValue
From	ModelResource AS PE
Where	ModelID = @ModelId
And		ResourceTypeID = @ModelGroupField

If @GroupByField = 'ProgID'
	Begin
		Update #Item Set GroupID = 1
	End
Else
	Begin
		If (Select Sum(Convert(real,Correlationgroup)) From #Item) > 0
			Begin
				Update #Item Set GroupID = A.GroupID From (Select ItemId, DENSE_RANK() OVER (ORDER BY CorrelationGroup) AS GroupID From #Item) AS A Where #Item.ItemId = A.ItemID
			End
		Else
			Set	@SQL = 'Update #Item Set GroupID = A.GroupID From (Select ItemId, DENSE_RANK() OVER (ORDER BY ' + @GroupByField + ') AS GroupID From #Item) AS A Where #Item.ItemId = A.ItemID'
			exec sp_ExecuteSQL @SQL
	End



insert into LogDatabaseUsage values ('generateOasisFiles2','updated #item',getdate(),null)

--update #item set AreaPerilId = 0 where AreaPerilId is null
--update #item set VulnerabilityId = 0 where VulnerabilityId is null


declare @modeltypeid int = (Select modeltypeid from model where modelid = @modelid)

if @modeltypeid = 1
	begin

		update	#item 
		set		IsValid = 0
		where	AreaPerilId is null
		or		VulnerabilityId is null
		or		TIV = 0

		delete from #item where IsValid = 0

		update	#item 
		set AltItemID =A.AltItemID
		From	(
				select	itemid,
						DENSE_RANK() OVER (ORDER BY areaperilid, vulnerabilityid, itemid) AS AltItemID
				from	#item
				) AS A
		where	#item.itemid = A.itemid
	end

if @modeltypeid = 2 
	begin
		update	#item
		set		AltItemID = A.AltItemID
		From	(
				select	itemid,
						DENSE_RANK() OVER (ORDER BY locid, perilid, CoverageTypeId, areaperilid, vulnerabilityid) AS AltItemID
				from	#item
				) AS A
		where	#item.itemid = A.itemid

		--remove zero items from non-zero alt items
		delete from #item
		where	TIV = 0
		and		AltItemID in (select altitemid from #item where TIV > 0)

		--remove duplicate zero items from zero value alt items
		delete from #Item
		where	TIV = 0
		and		itemid in (select itemid from (select AltItemID, max(ItemId) as itemid from #item group by AltItemID having sum(TIV) = 0 and count(itemid)>1) as a) 

		update	#item 
		set		IsValid = 0
		where	AreaPerilId is null
		or		VulnerabilityId is null

		update	#item 
		set		IsValid = 0
		from	#item as i
		join	(
				select	LocId,
						PerilId,
						Sum(TIV) AS TIV
				From	#item
				group by
						LocId,
						PerilId
				having	Sum(TIV) = 0
				) as n
					on	i.LocId = n.LocId
					and	i.PerilId = n.PerilId

		
		update #item set AreaPerilId = 0, VulnerabilityId = 0 where IsValid = 0

		delete from #item where locid in (select locid from #item group by LocId having sum(convert(int,isvalid)) = 0)

		--reindex
		update	#item
		set		AltItemID = A.AltItemID
		From	(
				select	itemid,
						DENSE_RANK() OVER (ORDER BY AltItemID) AS AltItemID
				from	#item
				) AS A
		where	#item.itemid = A.itemid
	
		update #item set AreaPerilId = 0 where AreaPerilId is null
		update #item set VulnerabilityId = 0 where VulnerabilityId is null
		
	end


insert into LogDatabaseUsage values ('generateOasisFiles2','deleted from #item',getdate(),null)

--coverage id
Update #Item Set CoverageId = A.CoverageId From (Select ItemID, DENSE_RANK() OVER (ORDER BY LocId,CoverageTypeId) AS CoverageId From #Item) AS A Where #Item.ItemID = A.ItemID

--PolicyID
Create Table #TempPolicy (PolicyId int null, InterestExposureID int null)

Insert Into #TempPolicy (PolicyId,InterestExposureID)
Select	Distinct P.PolicyID, CI.InterestExposureID
From	Policy AS P
Join	PolicyPeril as PP on P.PolicyID = PP.PolicyID
Join	PolicyCoverage AS PC on PP.PolicyPerilID = PC.PolicyPerilID
Join	CoverageItem AS CI on PC.PolicyCoverageID = CI.PolicyCoverageID
Join	#Item as I on CI.InterestExposureID = I.InterestExposureID
Where	P.ProgID = @ProgID

Update	#Item Set PolicyID = A.PolicyId 
From	#TempPolicy AS A
Where	#Item.InterestExposureID = A.InterestExposureID



insert into LogDatabaseUsage values ('generateOasisFiles2','temp policy',getdate(),null)


--AccountNumber
Set @ProfileElementID = (Select ProfileElementID From ProfileElement Where ProfileID = @CanLocProfileID and FieldID = @AccountNumber)
Select	@SQL = 'Update #Item Set AccountNumber = FieldValue From	' + TableName + ' Where ProfileElementId = ' + convert(nvarchar,@ProfileElementID) + ' And #Item.' + Replace(TableName,'Values','ID') + ' = ' + TableName + '.' + Replace(TableName,'Values','ID') 
From	ProfileElement AS PE
Join	[Table] AS T on PE.TableID = T.TableID
Where	ProfileID = @CanLocProfileID 
And		PE.FieldID = @AccountNumber
exec sp_ExecuteSQL @SQL


Update	#Item 
Set		SubLimitRef = PCV.FieldValue 
From	CoverageItem AS CI
Join	PolicyCoverage AS PC on CI.PolicyCoverageID = PC.PolicyCoverageID
Join	PolicyCoverageValues AS PCV on PC.PolicyCoverageID = PCV.PolicyCoverageID
Where	PCV.ProfileElementID = (Select ProfileElementID From ProfileElement Where ProfileID = @CanLocProfileID and FieldID = @SubLimitRef)
And		#Item.InterestExposureID = CI.InterestExposureID


DECLARE @GUID_CLEAN varchar(max);
DECLARE @GUID_Value UNIQUEIDENTIFIER = NEWID()
set @GUID_CLEAN  = replace(cast(@GUID_Value as varchar(max)), '-', '')
	DECLARE @tablename varchar(max)

	
	SET @SQL =
	'truncate table' + 'dbo.FlamingoCOVERAGES_'+@GUID_CLEAN
	SET @SQL =
	'truncate table' + 'dbo.FlamingoITEMS_'+@GUID_CLEAN
	SET @SQL =
	'truncate table' + 'dbo.FlamingoFMPROGRAMME_'+@GUID_CLEAN
	SET @SQL =
	'truncate table' + 'dbo.FlamingoFMPOLICYTC_'+@GUID_CLEAN
	SET @SQL =
	'trucate table' + 'dbo.FlamingoFMPROFILE_'+@GUID_CLEAN
	SET @SQL =
	'truncate table' + 'dbo.FlamingoFMXREF_'+@GUID_CLEAN
	SET @SQL =
	'truncate table' + 'dbo.FlamingoFMDICT_'+@GUID_CLEAN

--Truncate Table OasisITEMS
--Truncate Table OasisCOVERAGES
--Truncate Table OasisITEMDICT
--Truncate Table OasisFM_PROGRAMME
--Truncate Table OasisFM_POLICYTC
--Truncate Table OasisFM_PROFILE
--Truncate Table OasisFM_XREF
--Truncate Table OasisFMDICT
--print @GUID_CLEAN
exec CreateTempFlamingoTables @GUID_CLEAN

--OasisITEMS
--Insert Into OasisITEMS
set @sql = 'Insert Into FlamingoITEMS_'+@GUID_CLEAN+'
Select	ItemID,
		CoverageID,
		AreaPerilId,
		VulnerabilityId,
		GroupId
From	(
		Select	AltItemID AS ItemID,
				CoverageID,
				AreaPerilId,
				VulnerabilityId,
				Sum(TIV) AS TIV,
				GroupId
		From	#Item
		--Where	AreaPerilId <> 0
		Group By
				AltItemID,
				CoverageID,
				AreaPerilId,
				VulnerabilityId,
				GroupId
		) AS A'
exec sp_executesql @sql


--OasisCOVERAGES
--Insert Into  OasisCOVERAGES
set @sql = 'Insert into FlamingoCOVERAGES_'+@GUID_CLEAN+'
Select	CoverageID,
		Max(TIV) AS TIV
From	(
		Select	AltItemID AS ItemID,
				CoverageID,
				AreaPerilId,
				VulnerabilityId,
				Sum(TIV) AS TIV,
				GroupId
		From	#Item
		--Where	AreaPerilId <> 0
		Group By
				AltItemID,
				CoverageID,
				AreaPerilId,
				VulnerabilityId,
				GroupId
		) AS A
Group By 
		CoverageId'
exec sp_executesql @sql

--OasisITEMDICT
--Insert Into OasisITEMDICT
set @sql = 'Insert into FlamingoITEMDICT_'+@GUID_CLEAN+'
Select	Distinct AltItemID AS item_id,
		CoverageID AS coverage_id,
		dense_rank() over (order by AccountNumber + ''-'' + SourceLocNumber) AS location_id,
		--AccountNumber + ''-'' + SourceLocNumber AS location_desc,
		SourceLocNumber AS location_desc,
		dense_rank() over (order by LineOfBusiness) AS lob_id,
		LineOfBusiness AS lob_desc,
		dense_rank() over (order by StateCode, CountyCode) AS county_id,
		StateCode + ''-'' +  CountyCode as county_desc,
		dense_rank() over (order by StateCode) AS state_id,
		StateCode AS state_desc,
		dense_rank() over (order by PortfolioNumber) AS portfolionumber_id,
		PortfolioNumber AS portfolionumber_desc,
		dense_rank() over (order by LocationGroup) AS locationgroup_id,
		LocationGroup AS locationgroup_desc
From	#Item'
exec sp_executesql @sql

insert into LogDatabaseUsage values ('generateOasisFiles2','done item tables',getdate(),null)



--FM Files Calc
Declare @InterestTable nvarchar(255)
Declare @ValueTable nvarchar(255)
Declare	@InterestFieldId int
Declare	@ValueFieldDedId int
Declare	@ValueFieldLimId int
Declare @LevelID int 

CREATE TABLE #FM (ITEM_ID int NULL, ALTITEM_ID int NULL, LEVEL_ID int NULL, LAYER_ID int NULL, AGG_ID int NULL, POLICYTC_ID int NULL, DEDUCTIBLE FLOAT NULL, 
					LIMIT FLOAT NULL, SHARE_PROP_OF_LIM FLOAT NULL, DEDUCTIBLE_TYPE NVARCHAR(2) NULL, CALCRULE_ID INT NULL, TIV FLOAT NULL)
--CREATE TABLE #STAGINGFM (INTEREST_ID INT NULL, LAYER_ID int NULL, DEDUCTIBLE FLOAT NULL, LIMIT FLOAT NULL, ElementDimensionID int NULL)
--CREATE TABLE #STAGINGFM2 (INTEREST_ID INT NULL, LAYER_ID int NULL, DEDUCTIBLE FLOAT NULL, LIMIT FLOAT NULL, LAYER_NAME NVARCHAR(255) NULL)

CREATE INDEX Idx_FM_ITEM_ID ON #FM (ITEM_ID)
CREATE INDEX Idx_FM_LEVEL_ID ON #FM (LEVEL_ID)
--CREATE INDEX Idx_STAGINGFM_InterestRiskID ON #STAGINGFM (Interest_ID)

--level 1, coverage level
insert into #FM (ITEM_ID,ALTITEM_ID,LEVEL_ID,LAYER_ID,AGG_ID,POLICYTC_ID,DEDUCTIBLE,LIMIT,SHARE_PROP_OF_LIM,DEDUCTIBLE_TYPE,CALCRULE_ID,TIV)
	select ItemId,AltItemID,1,1,DENSE_RANK() OVER (ORDER BY coverageid) AS AGG_ID,NULL,NULL,NULL,NULL,'B',NULL,TIV from #Item

--level 2, combined level
insert into #FM (ITEM_ID,ALTITEM_ID,LEVEL_ID,LAYER_ID,AGG_ID,POLICYTC_ID,DEDUCTIBLE,LIMIT,SHARE_PROP_OF_LIM,DEDUCTIBLE_TYPE,CALCRULE_ID,TIV)
	select ItemId,AltItemID,2,1,DENSE_RANK() OVER (ORDER BY case when mct.coveragetypeid =4 then 2 else 1 end, locid) AS AGG_ID,NULL,NULL,NULL,NULL,'B',NULL,TIV from #Item
		left join ModelCoverageType mct on #Item.coveragetypeid = mct.ModelCoverageTypeId

--level 3, location level
insert into #FM (ITEM_ID,ALTITEM_ID,LEVEL_ID,LAYER_ID,AGG_ID,POLICYTC_ID,DEDUCTIBLE,LIMIT,SHARE_PROP_OF_LIM,DEDUCTIBLE_TYPE,CALCRULE_ID,TIV)
	select ItemId,AltItemID,3,1,DENSE_RANK() OVER (ORDER BY locid) AS AGG_ID,NULL,NULL,NULL,NULL,'B',NULL,TIV from #Item
	
--level 4, sublimit level
insert into #FM (ITEM_ID,ALTITEM_ID,LEVEL_ID,LAYER_ID,AGG_ID,POLICYTC_ID,DEDUCTIBLE,LIMIT,SHARE_PROP_OF_LIM,DEDUCTIBLE_TYPE,CALCRULE_ID,TIV)
	select ItemId,AltItemID,4,1,DENSE_RANK() OVER (ORDER BY PolicyID,sublimitref) AS AGG_ID,NULL,NULL,NULL,NULL,'MI',NULL,TIV from #Item
	
--level 5, policy level
insert into #FM (ITEM_ID,ALTITEM_ID,LEVEL_ID,LAYER_ID,AGG_ID,POLICYTC_ID,DEDUCTIBLE,LIMIT,SHARE_PROP_OF_LIM,DEDUCTIBLE_TYPE,CALCRULE_ID,TIV)
	select ItemId,AltItemID,5,1,DENSE_RANK() OVER (ORDER BY PolicyID) AS AGG_ID,NULL,NULL,NULL,NULL,'B',NULL,TIV from #Item
	
--level 6, layer level
insert into #FM (ITEM_ID,ALTITEM_ID,LEVEL_ID,LAYER_ID,AGG_ID,POLICYTC_ID,DEDUCTIBLE,LIMIT,SHARE_PROP_OF_LIM,DEDUCTIBLE_TYPE,CALCRULE_ID,TIV)
	select ItemId,AltItemID,6,PL.LayerNumber,DENSE_RANK() OVER (ORDER BY I.PolicyID) AS AGG_ID,NULL,NULL,NULL,NULL,'B',NULL,TIV from #Item as i
		join PolicyLayer AS PL ON I.PolicyID = PL.PolicyID 



insert into LogDatabaseUsage values ('generateOasisFiles2','created #FM',getdate(),null)

create table #exposures (InterestExposureID int null, CoverageItemID int null, ElementDimensionID int null,
					CoverageTypeID int null, PerilID int null)

insert into #exposures
select	ie.InterestExposureID,
		ci.CoverageItemID,
		pvd.ElementDimensionID,
		pvd.CoverageTypeID,
		pvd.PerilID
from	interestexposure as ie
join	interestexposurevalues as iev on ie.InterestExposureID = iev.InterestExposureID
join	ProfileElement as pe on iev.ProfileElementID = pe.ProfileElementID
join	ProfileValueDetail as pvd on pe.ProfileElementID = pvd.ProfileElementID
join	CoverageItem as ci on ie.InterestExposureID = ci.InterestExposureID
Join	#Item as I on CI.InterestExposureID = I.InterestExposureID
where	pe.FieldID = @TIVFieldID



create table #coverages (PolicyCoverageID int null, CoverageItemID int null, ElementDimensionID int null,
					CoverageTypeID int null, PerilID int null, FieldID int null, FieldValue float null)

insert into #coverages
select	distinct pc.PolicyCoverageID,
		ci.CoverageItemID,
		pvd.ElementDimensionID,
		pvd.CoverageTypeID,
		pvd.PerilID,
		pe.FieldID,
		case when pe.FieldID in (15,16,17,18,19,20,55,56) then pcv.FieldValue else cast(0 as float) end as FieldValue
from	PolicyCoverage as pc
join	PolicyCoverageValues as pcv on pc.PolicyCoverageID = pcv.PolicyCoverageID
join	ProfileElement as pe on pcv.ProfileElementID = pe.ProfileElementID
left join	
		ProfileValueDetail as pvd on pe.ProfileElementID = pvd.ProfileElementID
join	CoverageItem as ci on pc.PolicyCoverageID = ci.PolicyCoverageID
Join	#Item as I on CI.InterestExposureID = I.InterestExposureID
where	FieldID in (15, --coverage limit
					16, --coverage deductible
					17, --site limit
					18, --site deductible
					19, --combined limit
					20, --combined deductible
					--,23, --layer attachment point
					--24, --layer limit
					--27, --blanket deductible
					--28  --blanket limit
					55,
					56
					)



--level 1
Set @Level = 1
update #fm set LIMIT = FieldValue from (
				select	i.ItemId,
						c.FieldValue 
				from	#exposures as e 
				join	#coverages as c 
							on	e.CoverageItemID = c.CoverageItemID 
							and e.ElementDimensionID = c.ElementDimensionID
				join	#item as i
							on	i.InterestExposureID = e.InterestExposureID
							and	i.ElementDimensionID = e.ElementDimensionID
				where  c.FieldID = 15) As A
where	#fm.ITEM_ID = A.ItemId
and		#fm.LEVEL_ID = @Level



update #fm set DEDUCTIBLE = FieldValue from (
				select	i.ItemId,
						c.FieldValue 
				from	#exposures as e 
				join	#coverages as c 
							on	e.CoverageItemID = c.CoverageItemID 
							and e.ElementDimensionID = c.ElementDimensionID
				join	#item as i
							on	i.InterestExposureID = e.InterestExposureID
							and	i.ElementDimensionID = e.ElementDimensionID
				where  c.FieldID = 16) As A
where	#fm.ITEM_ID = A.ItemId
and		#fm.LEVEL_ID = @Level



--level 2
set @level = @level +1
update #fm set LIMIT = FieldValue from (
				select	i.ItemId,
						case when mct.coveragetypeid = 4 then 0 else c.FieldValue end as FieldValue
				from	#exposures as e 
				join	#coverages as c 
							on	e.CoverageItemID = c.CoverageItemID 
				join	#item as i
							on	i.InterestExposureID = e.InterestExposureID
				left join ModelCoverageType as mct 
							on i.coveragetypeid = mct.ModelCoverageTypeId
				where  c.FieldID = 19) As A
where	#fm.ITEM_ID = A.ItemId
and		#fm.LEVEL_ID = @level


update #fm set DEDUCTIBLE = FieldValue from (
				select	i.ItemId,
						case when mct.coveragetypeid = 4 then 0 else c.FieldValue end as FieldValue
				from	#exposures as e 
				join	#coverages as c 
							on	e.CoverageItemID = c.CoverageItemID 
				join	#item as i
							on	i.InterestExposureID = e.InterestExposureID
				left join ModelCoverageType as mct 
							on i.coveragetypeid = mct.ModelCoverageTypeId
				where  c.FieldID = 20) As A
where	#fm.ITEM_ID = A.ItemId
and		#fm.LEVEL_ID = @level



--level 3
set @level = @level +1
update #fm set LIMIT = FieldValue from (
				select	i.ItemId,
						FieldValue
				from	#exposures as e 
				join	#coverages as c 
							on	e.CoverageItemID = c.CoverageItemID 
				join	#item as i
							on	i.InterestExposureID = e.InterestExposureID
				where  c.FieldID = 17) As A
where	#fm.ITEM_ID = A.ItemId
and		#fm.LEVEL_ID = @level



update #fm set DEDUCTIBLE = FieldValue from (
				select	i.ItemId,
						FieldValue
				from	#exposures as e 
				join	#coverages as c 
							on	e.CoverageItemID = c.CoverageItemID 
				join	#item as i
							on	i.InterestExposureID = e.InterestExposureID
				where  c.FieldID = 18) As A
where	#fm.ITEM_ID = A.ItemId
and		#fm.LEVEL_ID = @level


--level 4
set @level = @level +1
update #fm set LIMIT = FieldValue from (
				select	i.ItemId,
						FieldValue
				from	#exposures as e 
				join	#coverages as c 
							on	e.CoverageItemID = c.CoverageItemID 
				join	#item as i
							on	i.InterestExposureID = e.InterestExposureID
				where  c.FieldID = 55) As A
where	#fm.ITEM_ID = A.ItemId
and		#fm.LEVEL_ID = @level



update #fm set DEDUCTIBLE = FieldValue from (
				select	i.ItemId,
						FieldValue
				from	#exposures as e 
				join	#coverages as c 
							on	e.CoverageItemID = c.CoverageItemID 
				join	#item as i
							on	i.InterestExposureID = e.InterestExposureID
				where  c.FieldID = 56) As A
where	#fm.ITEM_ID = A.ItemId
and		#fm.LEVEL_ID = @level



--level 5
set @level = @level +1
update #fm set LIMIT = FieldValue from (
				select	distinct i.ItemId,
						plv.FieldValue
				from	#item as i
				join	[Policy] as p on i.PolicyID = p.PolicyID
				join	PolicyLayer as pl on p.PolicyID = pl.PolicyID
				join	PolicyLayerValues as plv on pl.PolicyLayerID = plv.PolicyLayerID
				join	ProfileElement as pe on plv.ProfileElementID = pe.ProfileElementID
				where	pe.FieldID = 28
				) As A
where	#fm.ITEM_ID = A.ItemId
and		#fm.LEVEL_ID = @level


--MIN ded
update #fm set DEDUCTIBLE = FieldValue, DEDUCTIBLE_TYPE = 'MI' from (
				select	distinct i.ItemId,
						plv.FieldValue
				from	#item as i
				join	[Policy] as p on i.PolicyID = p.PolicyID
				join	PolicyLayer as pl on p.PolicyID = pl.PolicyID
				join	PolicyLayerValues as plv on pl.PolicyLayerID = plv.PolicyLayerID
				join	ProfileElement as pe on plv.ProfileElementID = pe.ProfileElementID
				where	pe.FieldID = 25
				and		plv.FieldValue > '0'
				) As A
where	#fm.ITEM_ID = A.ItemId
and		#fm.LEVEL_ID = @level

--MAX ded
update #fm set DEDUCTIBLE = FieldValue, DEDUCTIBLE_TYPE = 'MA' from (
				select	distinct i.ItemId,
						plv.FieldValue
				from	#item as i
				join	[Policy] as p on i.PolicyID = p.PolicyID
				join	PolicyLayer as pl on p.PolicyID = pl.PolicyID
				join	PolicyLayerValues as plv on pl.PolicyLayerID = plv.PolicyLayerID
				join	ProfileElement as pe on plv.ProfileElementID = pe.ProfileElementID
				where	pe.FieldID = 26
				and		plv.FieldValue > '0'
				) As A
where	#fm.ITEM_ID = A.ItemId
and		#fm.LEVEL_ID = @level

--BLANKET ded
update #fm set DEDUCTIBLE = FieldValue, DEDUCTIBLE_TYPE = 'B' from (
				select	distinct i.ItemId,
						plv.FieldValue
				from	#item as i
				join	[Policy] as p on i.PolicyID = p.PolicyID
				join	PolicyLayer as pl on p.PolicyID = pl.PolicyID
				join	PolicyLayerValues as plv on pl.PolicyLayerID = plv.PolicyLayerID
				join	ProfileElement as pe on plv.ProfileElementID = pe.ProfileElementID
				where	pe.FieldID = 27
				and		plv.FieldValue > '0'
				) As A
where	#fm.ITEM_ID = A.ItemId
and		#fm.LEVEL_ID = @level

insert into LogDatabaseUsage values ('generateOasisFiles2','done #FM level 5',getdate(),null)

--level 6
set @level = @level +1

select	i.ItemId,
		pl.LayerNumber,
		plv.FieldValue
into	tempA
from	#item as i
join	[Policy] as p on i.PolicyID = p.PolicyID
join	PolicyLayer as pl on p.PolicyID = pl.PolicyID
join	PolicyLayerValues as plv on pl.PolicyLayerID = plv.PolicyLayerID
join	ProfileElement as pe on plv.ProfileElementID = pe.ProfileElementID
where	pe.FieldID = 24

update	#fm 
set		LIMIT = FieldValue 
from	tempA As A
where	#fm.ITEM_ID = A.ItemId
and		#fm.LAYER_ID = A.LayerNumber
and		#fm.LEVEL_ID = @level

insert into LogDatabaseUsage values ('generateOasisFiles2','done #FM level 6 Limit',getdate(),null)


select	i.ItemId,
		pl.LayerNumber,
		plv.FieldValue
into	tempB
from	#item as i
join	[Policy] as p on i.PolicyID = p.PolicyID
join	PolicyLayer as pl on p.PolicyID = pl.PolicyID
join	PolicyLayerValues as plv on pl.PolicyLayerID = plv.PolicyLayerID
join	ProfileElement as pe on plv.ProfileElementID = pe.ProfileElementID
where	pe.FieldID = 23

update	#fm 
set		DEDUCTIBLE = FieldValue 
from	tempB As A
where	#fm.ITEM_ID = A.ItemId
and		#fm.LAYER_ID = A.LayerNumber
and		#fm.LEVEL_ID = @level

insert into LogDatabaseUsage values ('generateOasisFiles2','done #FM level 6 Ded',getdate(),null)

select	i.ItemId,		
		pl.LayerNumber,		
		plv.FieldValue		
into	tempC		
from	#item as i		
join	[Policy] as p on i.PolicyID = p.PolicyID		
join	PolicyLayer as pl on p.PolicyID = pl.PolicyID		
join	PolicyLayerValues as plv on pl.PolicyLayerID = plv.PolicyLayerID		
join	ProfileElement as pe on plv.ProfileElementID = pe.ProfileElementID		
where	pe.FieldID = 57		
			
update	#fm 
set		SHARE_PROP_OF_LIM = FieldValue 
from 	tempC As A		
where	#fm.ITEM_ID = A.ItemId		
and		#fm.LAYER_ID = A.LayerNumber		
and		#fm.LEVEL_ID = @level		

insert into LogDatabaseUsage values ('generateOasisFiles2','done #FM level 6 Share',getdate(),null)

--sum financials across coverages to altitem_id level
update	#fm 
set		deductible = a.deductible, 
		limit = a.limit
from	(select ALTITEM_ID, sum(deductible) as deductible, sum(limit) as limit from #fm where LEVEL_ID = 1 group by ALTITEM_ID) as a
where	#fm.ALTITEM_ID = a.ALTITEM_ID
and		#fm.LEVEL_ID = 1

--------------------
--fixes
--------------------
--nulls
update	#FM set SHARE_PROP_OF_LIM = 0 where SHARE_PROP_OF_LIM is null
update	#FM set limit = 0 where limit is null
update	#FM set DEDUCTIBLE = 0 where DEDUCTIBLE is null

----share/limit combinations
--share > 1 and limit > 1, share = share/limit
update  #FM set SHARE_PROP_OF_LIM = 
				case when SHARE_PROP_OF_LIM > 1 and LIMIT > 1 then SHARE_PROP_OF_LIM/LIMIT
				else SHARE_PROP_OF_LIM end
--share > 1 and limit = 0, limit = share
update  #FM set LIMIT = SHARE_PROP_OF_LIM,
				SHARE_PROP_OF_LIM = 0
			where SHARE_PROP_OF_LIM > 1
			and	 LIMIT = 0
--share > 1 and limit < 1, limit=share/limit (weird), share = limit
update  #FM set LIMIT = SHARE_PROP_OF_LIM/LIMIT,
				SHARE_PROP_OF_LIM = LIMIT
			where SHARE_PROP_OF_LIM > 1
			and		LIMIT < 1
			and		LIMIT > 0
			
--get agg total tivs
Select	LEVEL_ID, LAYER_ID, AGG_ID, sum(TIV) AS TOTAL_TIV
into	#tivtotals
From	#FM
Group By
		LEVEL_ID, LAYER_ID, AGG_ID

--set limit = TIV where share required
update	#FM
set		LIMIT = TOTAL_TIV
from	#FM AS F
join	#tivtotals as t
			on	f.LEVEL_ID = t.LEVEL_ID
			and	f.LAYER_ID = t.LAYER_ID
			and	f.AGG_ID = t.AGG_ID
where	LIMIT = 0 
and		SHARE_PROP_OF_LIM >0
and		SHARE_PROP_OF_LIM <=1

--set % deductibles to absolute for min/max
update	#FM
set		DEDUCTIBLE = DEDUCTIBLE * TOTAL_TIV
from	#FM AS F
join	#tivtotals as t
			on	f.LEVEL_ID = t.LEVEL_ID
			and	f.LAYER_ID = t.LAYER_ID
			and	f.AGG_ID = t.AGG_ID
where	DEDUCTIBLE > 0 
and		DEDUCTIBLE <= 1
--and		DEDUCTIBLE_TYPE IN ('MI','MA')

--set limit = % TIV where % with no limit
update	#FM
set		LIMIT = LIMIT * TOTAL_TIV
from	#FM AS F
join	#tivtotals as t
			on	f.LEVEL_ID = t.LEVEL_ID
			and	f.LAYER_ID = t.LAYER_ID
			and	f.AGG_ID = t.AGG_ID
where	LIMIT > 0 
and		LIMIT <= 1
and		SHARE_PROP_OF_LIM = 0

--------------------
--calc rules
--------------------
update	#FM
set		CALCRULE_ID = CASE	
			WHEN LIMIT >1 AND SHARE_PROP_OF_LIM =0 AND DEDUCTIBLE >1 AND DEDUCTIBLE_TYPE = 'B' THEN 1
			WHEN LIMIT =0 AND SHARE_PROP_OF_LIM >0 AND SHARE_PROP_OF_LIM <=1 AND DEDUCTIBLE =0 THEN 2
			WHEN LIMIT >1 AND SHARE_PROP_OF_LIM >0 AND SHARE_PROP_OF_LIM <=1 AND DEDUCTIBLE =0 THEN 2
			WHEN LIMIT =0 AND SHARE_PROP_OF_LIM >0 AND SHARE_PROP_OF_LIM <=1 AND DEDUCTIBLE >1 AND DEDUCTIBLE_TYPE = 'B' THEN 2
			WHEN LIMIT >1 AND SHARE_PROP_OF_LIM >0 AND SHARE_PROP_OF_LIM <=1 AND DEDUCTIBLE >1 AND DEDUCTIBLE_TYPE = 'B' THEN 2
			WHEN LIMIT >1 AND SHARE_PROP_OF_LIM =0 AND DEDUCTIBLE >0 AND DEDUCTIBLE<=1 AND DEDUCTIBLE_TYPE = 'B' THEN 4
			WHEN LIMIT >0 AND LIMIT <=1 AND SHARE_PROP_OF_LIM =0 AND DEDUCTIBLE >1 AND DEDUCTIBLE_TYPE = 'B' THEN 5
			WHEN LIMIT =0 AND SHARE_PROP_OF_LIM =0 AND DEDUCTIBLE >0 AND DEDUCTIBLE <=1 AND DEDUCTIBLE_TYPE = 'B' THEN 6
			WHEN LIMIT >1 AND SHARE_PROP_OF_LIM =0 AND DEDUCTIBLE >1 AND DEDUCTIBLE_TYPE = 'MA' THEN 7
			WHEN LIMIT >1 AND SHARE_PROP_OF_LIM =0 AND DEDUCTIBLE >1 AND DEDUCTIBLE_TYPE = 'MI' THEN 8
			WHEN LIMIT =0 AND SHARE_PROP_OF_LIM =0 AND DEDUCTIBLE >1 AND DEDUCTIBLE_TYPE = 'MA' THEN 10
			WHEN LIMIT =0 AND SHARE_PROP_OF_LIM =0 AND DEDUCTIBLE >1 AND DEDUCTIBLE_TYPE = 'MI' THEN 11
			WHEN LIMIT =0 AND SHARE_PROP_OF_LIM =0 AND DEDUCTIBLE =0 THEN 12
			WHEN LIMIT =0 AND SHARE_PROP_OF_LIM =0 AND DEDUCTIBLE >1 AND DEDUCTIBLE_TYPE = 'B' THEN 12
			WHEN LIMIT >1 AND SHARE_PROP_OF_LIM =0 AND DEDUCTIBLE =0  THEN 14
			WHEN LIMIT >0 AND LIMIT <=1 AND SHARE_PROP_OF_LIM =0 AND DEDUCTIBLE =0 THEN 15
			WHEN LIMIT =0 AND SHARE_PROP_OF_LIM >0 AND SHARE_PROP_OF_LIM <=1 AND DEDUCTIBLE >0 AND DEDUCTIBLE <=1 AND DEDUCTIBLE_TYPE = 'B' THEN 18
			WHEN LIMIT >1 AND SHARE_PROP_OF_LIM >0 AND SHARE_PROP_OF_LIM <=1 AND DEDUCTIBLE >0 AND DEDUCTIBLE <=1 AND DEDUCTIBLE_TYPE = 'B' THEN 18
			ELSE 2 END -- decide on better default


--set policytc ids
create table #PolTC (POLICYTC_ID int identity(1,1), CALCRULE_ID int, deductible float null, limit float null, SHARE_PROP_OF_LIM float null)		
insert into #PolTC		
select 12,0,0,0		
union		
select distinct CALCRULE_ID, deductible, limit, SHARE_PROP_OF_LIM from #FM		
		
update	#FM set POLICYTC_ID = A.POLICYTC_ID 		
from	#PolTC AS A		
where	#FM.CALCRULE_ID = a.CALCRULE_ID		
and		#FM.deductible = a.deductible		
and		#FM.limit = a.limit		
and		#FM.SHARE_PROP_OF_LIM = a.SHARE_PROP_OF_LIM


--correct (index) levels to run in oasis new FM
declare @zeropoltcid int = (select PolicyTC_ID from #PolTC where CALCRULE_ID=12 and deductible=0 and limit=0 and SHARE_PROP_OF_LIM=0)
Create Table #EmptyLevels (Level_ID int null)
Insert Into #EmptyLevels
Select	Level_ID
From	(
		Select	Level_ID,
				Sum(CASE When PolicyTC_ID = @zeropoltcid Then 1 Else 0 End) AS StubPolTC,
				Count(PolicyTC_ID) AS PolTC
		From	#FM
		Where	Level_ID not in (1,6)
		Group By
				Level_ID
		) AS A
Where	StubPolTC = PolTC

select	distinct LEVEL_ID,
		dense_rank() over (order by LEVEL_ID) as newLEVEL_ID
into	#level
from	#FM
where	LEVEL_ID not in (select LEVEL_ID from #EmptyLevels)



delete from #FM where LEVEL_ID in (select LEVEL_ID from #EmptyLevels)
update #FM set LEVEL_ID = newLEVEL_ID from #level where #FM.LEVEL_ID = #level.LEVEL_ID

--select * from #fm order by LEVEL_ID, AGG_ID

insert into LogDatabaseUsage values ('generateOasisFiles2','updated #FM',getdate(),null)

----FM Tables

--FM_Programme
--level1

set @sql = 'Insert into FlamingoFMPROGRAMME_'+@GUID_CLEAN+'
Select	Distinct ALTITEM_ID,
		LEVEL_ID,
		AGG_ID
From	#FM
where	LEVEL_ID = 1'
exec sp_executesql @sql

Set	@Level = 1
Select @MaxLevel = max(LEVEL_ID) From #FM

--upper levels
while @Level <= @MaxLevel
begin
set @sql = 'Insert into FlamingoFMPROGRAMME_'+@GUID_CLEAN+'
	Select	Distinct A.AGG_ID,
			B.LEVEL_ID,
			B.AGG_ID
	From	#FM AS A
	Join	#FM AS B
				on	A.ITEM_ID = B.ITEM_ID
	where	A.LEVEL_ID = ' + cast(@Level as nvarchar) + '
	And		B.LEVEL_ID = ' + cast(@Level+1 as nvarchar)

	exec sp_executesql @sql

	Set		@Level = @Level+1
end



--FM_PolicyTC
--Insert into OasisFM_POLICYTC (LAYER_ID,LEVEL_ID,AGG_ID,POLICYTC_ID)
set @sql = 'Insert into FlamingoFMPOLICYTC_'+@GUID_CLEAN+'
Select	Distinct LAYER_ID,
		LEVEL_ID,
		AGG_ID,
		POLICYTC_ID
From	#FM'
exec sp_executesql @sql
 
--FM_Profile
--Insert into OasisFM_PROFILE (profile_id,
set @sql = 'Insert into FlamingoFMPROFILE_'+@GUID_CLEAN+'

Select	Distinct POLICYTC_ID,
		CALCRULE_ID,
		CASE WHEN DEDUCTIBLE_TYPE = ''B'' THEN DEDUCTIBLE ELSE 0 END,
		CASE WHEN DEDUCTIBLE_TYPE = ''MI'' THEN DEDUCTIBLE ELSE 0 END,
		CASE WHEN DEDUCTIBLE_TYPE = ''MA'' THEN DEDUCTIBLE ELSE 0 END,
		0,
		LIMIT,
		SHARE_PROP_OF_LIM,
		0,
		0
From	#FM'
exec sp_executesql @sql


--FM_XRef
Create Table #FMXRef (Item_Id int null, OUTPUT_ID int null,AGG_ID int null,LAYER_ID int null,PolicyID int null, policy_name nvarchar(255) null,layer_name nvarchar(255) null)
insert into #FMXRef (Item_ID,OUTPUT_ID,AGG_ID,LAYER_ID,PolicyID)
Select	Distinct ALTITEM_ID, --Item_Id,
		DENSE_RANK() OVER(ORDER BY ALTITEM_ID,LAYER_ID) AS OUTPUT_ID,
		AGG_ID,
		LAYER_ID,
		PolicyID
From	(
		Select	A.Item_Id,
				ALTITEM_ID,
				AGG_ID,
				LAYER_ID,
				PolicyID
		From	(
				Select	Distinct Item_Id,
						ALTITEM_ID,
						--ALTITEM_ID AS AGG_ID,
						--CoverageId AS AGG_ID,
						i.PolicyID
				From	#FM as f
				join	#item as i on i.ItemId = f.ITEM_ID
				Where	LEVEL_ID = 1
				) AS A
		Join	(
				Select	Distinct Item_Id,
						AGG_ID,
						LAYER_ID
				From	#FM
				Where	LEVEL_ID = @MaxLevel 
				) AS B
				on A.Item_Id = B.Item_Id
		) AS C

Update	#FMXRef 
set		policy_name = accountnumber
from	#item
where	#FMXRef.item_id = #item.altitemid

Update	#FMXRef 
set		layer_name = PolicyLayerName
from	PolicyLayer
where	#FMXRef.PolicyID = PolicyLayer.PolicyID
and		#FMXRef.LAYER_ID = PolicyLayer.LayerNumber


--Insert into OasisFM_XRef (OUTPUT_ID,AGG_ID,LAYER_ID)
set @sql = 'Insert into FlamingoFMXREF_'+@GUID_CLEAN+'
Select Distinct OUTPUT_ID,Item_Id --AGG_ID
					,LAYER_ID From #FMXRef order by Item_Id --AGG_ID'
exec sp_executesql @sql

--FM Dict
--insert into OasisFMDICT (output_id,item_id,agg_id,layer_id,policy_name,layer_name)
set @sql = 'Insert into FlamingoFMDICT_'+@GUID_CLEAN+' 
Select Distinct OUTPUT_ID,ITEM_ID, AGG_ID,LAYER_ID,policy_name,layer_name From #FMXRef'
exec sp_executesql @sql


insert into LogDatabaseUsage values ('generateOasisFiles2','done FM Tables',getdate(),null)

--select * from #EmptyLevels
--select * from OasisFM_PROGRAMME
--select * from OasisFM_POLICYTC
--select * from OasisFM_PROFILE
--select * from OasisFM_XREF

--select * from #item
--select * from #fm

----drop temp tables
Drop Table #Item
Drop Table #TempPolicy

Drop Table #FM
Drop table #level
Drop Table #PolTC
Drop Table #EmptyLevels
Drop Table #FMXRef
Drop table #exposures
Drop table #coverages
Drop table #tivtotals

Drop table tempA
Drop table tempB
Drop table tempC

--update status
if @@error = 0
	begin
		update progoasis set [status] = 'Loaded' where progoasisid = @ProgOasisId
	end
else
	begin
		update progoasis set [status] = 'Oasis File Generation Error' where progoasisid = @ProgOasisId
	end

Select 'Done', @GUID_CLEAN 


GO
/****** Object:  StoredProcedure [dbo].[generateOasisFilesRecords]    Script Date: 8/7/2019 4:00:29 PM ******/
SET ANSI_NULLS ON
GO
SET QUOTED_IDENTIFIER ON
GO

CREATE PROCEDURE [dbo].[generateOasisFilesRecords] @ProgOasisId int, @LocationID int, @ItemsFileName nvarchar(255), @CoveragesFileName nvarchar(255), @ItemDictFileName nvarchar(255),
													@FMProgrammeFileName nvarchar(255), @FMPolicyTCFileName nvarchar(255), @FMProfileFileName nvarchar(255),
													@FMXRefFileName nvarchar(255), @FMDictFileName nvarchar(255)
AS
SET NOCOUNT ON;
----------------------------------------------------------------------------
--log database usage
declare	@ProcedureName nvarchar(255)  = (Select OBJECT_NAME(@@PROCID))
declare	@ParameterList nvarchar(2500) = '@ProgOasisId = ' + convert(nvarchar,@ProgOasisId) + ', @LocationID = ' + convert(nvarchar,@LocationID)
											+ ', @ItemsFileName = ' + @ItemsFileName
											+ ', @CoveragesFileName = ' + @CoveragesFileName
											+ ', @ItemDictFileName = ' + @ItemDictFileName
											+ ', @FMProgrammeFileName = ' + @FMProgrammeFileName
											+ ', @FMPolicyTCFileName = ' + @FMPolicyTCFileName
											+ ', @FMProfileFileName = ' + @FMProfileFileName
											+ ', @FMXRefFileName = ' + @FMXRefFileName
											+ ', @FMDictFileName = ' + @FMDictFileName
declare	@LogTimestamp datetime = getdate()

exec updateLogDatabaseUsage @ProcedureName,@ParameterList,@LogTimestamp
----------------------------------------------------------------------------
BEGIN

	Declare	@FileId int							= (Select ISNULL(Max(FileId),0) + 1 From [File])
	Declare	@ResourceId int
	Declare	@NextResourceId int					= (Select  ISNULL(Max(ResourceId),0) + 1 From [Resource])
	Declare	@FileResourceId int					= (Select  ISNULL(Max(FileResourceId),0) + 1 From [FileResource])
	Declare	@ProgId int							= (select ProgId from ProgOasis where progoasisid = @progoasisid)
	Declare	@ModelId int						= (select ModelId from ProgOasis where progoasisid = @progoasisid)
	Declare	@SessionID int						= (select SessionId from ProgOasis where progoasisid = @progoasisid)
	
--Oasis Items File
	Set		@ResourceId = (Select ResourceId From [Resource] Where ResourceTable = 'ProgOasis' And ResourceKey = @ProgOasisId And ResourceTypeID = 111) --Oasis Items File
	insert into Resource values (@NextResourceId,'ProgOasis',@ProgOasisId,NULL,211) --legacy file resource type
	update [fileresource] set resourceid = @NextResourceId where resourceid = @ResourceId
	insert into [file] values (@FileId,@ItemsFileName,'Items File ProgOasis ' + convert(nvarchar,@ProgOasisId),1,1,@LocationID,getdate(),getdate(),null,'Sys','Sys',NULL,111) --Oasis Items File
	insert into [FileResource] values (@FileResourceId,@FileId,@ResourceId)
	set		@FileResourceId = @FileResourceId+1
	set		@FileId = @FileId+1
	set		@NextResourceId = @NextResourceId+1

--Oasis Coverages File
	Set		@ResourceId = (Select ResourceId From Resource Where ResourceTable = 'ProgOasis' And ResourceKey = @ProgOasisId And ResourceTypeID = 112) --Oasis Coverages File
	insert into Resource values (@NextResourceId,'ProgOasis',@ProgOasisId,NULL,212) --legacy file resource type
	update [fileresource] set resourceid = @NextResourceId where resourceid = @ResourceId
	insert into [file] values (@FileId,@CoveragesFileName,'Coverages File ProgOasis ' + convert(nvarchar,@ProgOasisId),1,1,@LocationID,getdate(),getdate(),null,'Sys','Sys',NULL,112) --Oasis Coverages File
	insert into [FileResource] values (@FileResourceId,@FileId,@ResourceId)
	set		@FileResourceId = @FileResourceId+1
	set		@FileId = @FileId+1
	set		@NextResourceId = @NextResourceId+1

--Oasis Item Dictionary File
	Set		@ResourceId = (Select ResourceId From Resource Where ResourceTable = 'ProgOasis' And ResourceKey = @ProgOasisId And ResourceTypeID = 113) --Oasis Item Dictionary File
	insert into Resource values (@NextResourceId,'ProgOasis',@ProgOasisId,NULL,213) --legacy file resource type
	update [fileresource] set resourceid = @NextResourceId where resourceid = @ResourceId
	insert into [file] values (@FileId,@ItemDictFileName,'Item Dictionary File ProgOasis ' + convert(nvarchar,@ProgOasisId),1,1,@LocationID,getdate(),getdate(),null,'Sys','Sys',NULL,113) --Oasis Item Dictionary File
	insert into [FileResource] values (@FileResourceId,@FileId,@ResourceId)
	set		@FileResourceId = @FileResourceId+1
	set		@FileId = @FileId+1
	set		@NextResourceId = @NextResourceId+1

--Oasis FM Programme File
	Set		@ResourceId = (Select ResourceId From Resource Where ResourceTable = 'ProgOasis' And ResourceKey = @ProgOasisId And ResourceTypeID = 114) --Oasis FM Programme File
	insert into Resource values (@NextResourceId,'ProgOasis',@ProgOasisId,NULL,214) --legacy file resource type
	update [fileresource] set resourceid = @NextResourceId where resourceid = @ResourceId
	insert into [file] values (@FileId,@FMProgrammeFileName,'FM Programme File ProgOasis ' + convert(nvarchar,@ProgOasisId),1,1,@LocationID,getdate(),getdate(),null,'Sys','Sys',NULL,114) --Oasis FM Programme File
	insert into [FileResource] values (@FileResourceId,@FileId,@ResourceId)
	set		@FileResourceId = @FileResourceId+1
	set		@FileId = @FileId+1
	set		@NextResourceId = @NextResourceId+1

--Oasis FM Policy TC File
	Set		@ResourceId = (Select ResourceId From Resource Where ResourceTable = 'ProgOasis' And ResourceKey = @ProgOasisId And ResourceTypeID = 115) --Oasis FM Policy TC File
	insert into Resource values (@NextResourceId,'ProgOasis',@ProgOasisId,NULL,215) --legacy file resource type
	update [fileresource] set resourceid = @NextResourceId where resourceid = @ResourceId
	insert into [file] values (@FileId,@FMPolicyTCFileName,'FM Policy TC File ProgOasis ' + convert(nvarchar,@ProgOasisId),1,1,@LocationID,getdate(),getdate(),null,'Sys','Sys',NULL,115) --Oasis FM Policy TC File
	insert into [FileResource] values (@FileResourceId,@FileId,@ResourceId)
	set		@FileResourceId = @FileResourceId+1
	set		@FileId = @FileId+1
	set		@NextResourceId = @NextResourceId+1

--Oasis FM Profile File
	Set		@ResourceId = (Select ResourceId From Resource Where ResourceTable = 'ProgOasis' And ResourceKey = @ProgOasisId And ResourceTypeID = 116) --Oasis FM Profile File
	insert into Resource values (@NextResourceId,'ProgOasis',@ProgOasisId,NULL,216) --legacy file resource type
	update [fileresource] set resourceid = @NextResourceId where resourceid = @ResourceId
	insert into [file] values (@FileId,@FMProfileFileName,'FM Profile File ProgOasis ' + convert(nvarchar,@ProgOasisId),1,1,@LocationID,getdate(),getdate(),null,'Sys','Sys',NULL,116) --Oasis FM Profile File
	insert into [FileResource] values (@FileResourceId,@FileId,@ResourceId)
	set		@FileResourceId = @FileResourceId+1
	set		@FileId = @FileId+1
	set		@NextResourceId = @NextResourceId+1

--Oasis FM XRef File
	Set		@ResourceId = (Select ResourceId From Resource Where ResourceTable = 'ProgOasis' And ResourceKey = @ProgOasisId And ResourceTypeID = 117) --Oasis FM XRef File
	insert into Resource values (@NextResourceId,'ProgOasis',@ProgOasisId,NULL,217) --legacy file resource type
	update [fileresource] set resourceid = @NextResourceId where resourceid = @ResourceId
	insert into [file] values (@FileId,@FMXRefFileName,'FM XRef File ProgOasis ' + convert(nvarchar,@ProgOasisId),1,1,@LocationID,getdate(),getdate(),null,'Sys','Sys',NULL,117) --Oasis FM XRef File
	insert into [FileResource] values (@FileResourceId,@FileId,@ResourceId)
	set		@FileResourceId = @FileResourceId+1
	set		@FileId = @FileId+1
	set		@NextResourceId = @NextResourceId+1

--Oasis FM Dict File
	Set		@ResourceId = (Select ResourceId From Resource Where ResourceTable = 'ProgOasis' And ResourceKey = @ProgOasisId And ResourceTypeID = 127) --Oasis FM Dict File
	insert into Resource values (@NextResourceId,'ProgOasis',@ProgOasisId,NULL,227) --legacy file resource type
	update [fileresource] set resourceid = @NextResourceId where resourceid = @ResourceId
	insert into [file] values (@FileId, @FMDictFileName,'FM Dict File ProgOasis ' + convert(nvarchar,@ProgOasisId),1,1,@LocationID,getdate(),getdate(),null,'Sys','Sys',NULL,120) --Oasis FM Dict File
	insert into [FileResource] values (@FileResourceId,@FileId,@ResourceId)
	set		@FileResourceId = @FileResourceId+1
	set		@FileId = @FileId+1
	set		@NextResourceId = @NextResourceId+1

	declare @GUID_CLEAN varchar(max);
	


	DECLARE @sql nvarchar(max)
	SET @SQL =
	'drop table' + 'dbo.FlamingoCOVERAGES_'+@GUID_CLEAN+
	'drop table' + 'dbo.FlamingoITEMS_'+@GUID_CLEAN+
	'drop table' + 'dbo.FlamingoITEMDict_'+@GUID_CLEAN+
	'drop table' + 'dbo.FlamingoFM_PROGRAMME_'+@GUID_CLEAN+	
	'drop table' + 'dbo.FlamingoFM_POLICYTC_'+@GUID_CLEAN+
	'drop table' + 'dbo.FlamingoFM_PROFILE_'+@GUID_CLEAN+
	'drop table' + 'dbo.FlamingoFM_XREF_'+@GUID_CLEAN+
	'drop table' + 'dbo.OasisFMDict_'+@GUID_CLEAN

END









GO
