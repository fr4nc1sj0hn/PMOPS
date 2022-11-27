import pandas as pd
import numpy as np
import functools as ft
import warnings
warnings.filterwarnings('ignore')

def identify_reservations(df):
    try:
        Fab300_raw_reservations = df.copy()
        Fab300_raw_reservations["DATE_TIME_STAMP"] = pd.to_datetime(Fab300_raw_reservations["DATE_TIME_STAMP"])
        SortedRows = Fab300_raw_reservations.sort_values(["EVENT_ROW_ID"])

        index = range(1,len(SortedRows)+1)
        IndexShift_1 = [i-1 if i%2 != 0 else np.nan for i in index]
        IndexShift_2 = [i-1 if i%2 == 0 else np.nan for i in index]

        SortedRows["Index"] = index
        SortedRows["IndexShift_1"] = IndexShift_1
        SortedRows["IndexShift_2"] = IndexShift_2
        SortedRows["IndexShift_1_1"] = SortedRows["IndexShift_1"].fillna(method='bfill')
        SortedRows["IndexShift_2_1"] = SortedRows["IndexShift_2"].fillna(method='bfill')
        SortedRows = SortedRows.drop(columns=['IndexShift_1', 'IndexShift_2'])
        SortedRows = SortedRows.rename(columns={"IndexShift_1_1":"IndexShift_1","IndexShift_2_1":"IndexShift_2"})
        FilledUp2 = SortedRows.copy()

        UnpivotedOnlySelectedColumns = pd.melt(FilledUp2, id_vars=['FO_ROW_ID','Index','IndexShift_1','IndexShift_2'], 
                    value_vars=["ResWBS", "ResTk", "DATE_TIME_STAMP", "USER_ID", "EVENT_ROW_ID"])

        UnpivotedOnlySelectedColumns["ResProperty_1"] = np.where(UnpivotedOnlySelectedColumns['Index']==UnpivotedOnlySelectedColumns['IndexShift_1'], UnpivotedOnlySelectedColumns['variable'] + "_Start", UnpivotedOnlySelectedColumns['variable'] + "_End")
        UnpivotedOnlySelectedColumns["ResProperty_2"] = np.where(UnpivotedOnlySelectedColumns['Index']==UnpivotedOnlySelectedColumns['IndexShift_2'], UnpivotedOnlySelectedColumns['variable'] + "_Start", UnpivotedOnlySelectedColumns['variable'] + "_End")

        AddedCustom4 = UnpivotedOnlySelectedColumns.copy()
        RemovedColumns1 = AddedCustom4.drop(columns=["Index", "IndexShift_2", "variable", "ResProperty_2"])

        PivotedColumn1 = RemovedColumns1.pivot(index=['FO_ROW_ID','IndexShift_1'],columns='ResProperty_1',values='value').reset_index()

        if "ResWBS_Start" in PivotedColumn1.columns:
            FilteredRows01 = PivotedColumn1[(PivotedColumn1["ResWBS_Start"].notnull() & PivotedColumn1['ResWBS_Start'].str.len() > 0)]
        else:
            #empty dataframe
            FilteredRows01 = pd.DataFrame(columns=PivotedColumn1.columns)
            
        
        RemovedColumns2 = AddedCustom4.drop(columns=["Index", "IndexShift_1", "variable", "ResProperty_1"])
        PivotedColumn2 = RemovedColumns2.pivot(index=['FO_ROW_ID','IndexShift_2'],columns='ResProperty_2',values='value').reset_index()

        FilteredRows02 = PivotedColumn2[(PivotedColumn2["ResWBS_Start"].notnull() & PivotedColumn2['ResWBS_Start'].str.len() > 0)]
        
        if "ResWBS_Start" in PivotedColumn2.columns:
            FilteredRows02 = PivotedColumn2[(PivotedColumn2["ResWBS_Start"].notnull() & PivotedColumn2['ResWBS_Start'].str.len() > 0)]
        else:
            #empty dataframe
            FilteredRows02 = pd.DataFrame(columns=PivotedColumn2.columns)
        
        columns = [
            'FO_ROW_ID', 
            'DATE_TIME_STAMP_End',
            'DATE_TIME_STAMP_Start', 
            'EVENT_ROW_ID_End', 
            'EVENT_ROW_ID_Start',
            'ResTk_End', 
            'ResTk_Start', 
            'ResWBS_End', 
            'ResWBS_Start', 
            'USER_ID_End',
            'USER_ID_Start'
        ]
            
            
        if FilteredRows01.shape[0] == 0:
            combined = FilteredRows02[columns]
        elif FilteredRows02.shape[0] == 0:
            combined = FilteredRows01[columns]
        else:
            FilteredRows01 = FilteredRows01[columns]
            FilteredRows02 = FilteredRows02[columns]
        
        combined = pd.concat([FilteredRows01,FilteredRows02])

        combined = combined.rename(
            columns=
            {
                "ResTk_Start":"ResTk",
                "ResWBS_Start":"WBS",
                "EVENT_ROW_ID_Start": "EVENT_ROW_ID_Begin",
                "DATE_TIME_STAMP_Start": "DATE_TIME_STAMP_Begin"
            }
        )

        columns = [
            "FO_ROW_ID",
            "ResTk", 
            "WBS", 
            "EVENT_ROW_ID_Begin", 
            "EVENT_ROW_ID_End", 
            "DATE_TIME_STAMP_Begin", 
            "DATE_TIME_STAMP_End", 
            "USER_ID_Start", 
            "USER_ID_End"
        ]
        Tools_with_resersvations  = combined#[columns]

        final = combined[columns]
        return final
    except:
        return pd.DataFrame()


def processFab300RawReservations(df):
    columns = [
        "FO_ROW_ID",
        "DATE_TIME_STAMP_Begin",
        "DATE_TIME_STAMP_End",
        'EVENT_ROW_ID_Begin', 
        'EVENT_ROW_ID_End',
        'ResTk',
        'USER_ID_Start',
        'USER_ID_End',
        'WBS'
    ]

    for_row_ids = df["FO_ROW_ID"].unique()

    Tools_with_reservations = pd.DataFrame(columns=columns)
    for row_id in for_row_ids:
        grpdata = df[df["FO_ROW_ID"] == row_id]

        df_reserv = identify_reservations(grpdata)
        if df.shape[0] > 0:
            Tools_with_reservations = pd.concat([Tools_with_reservations,df_reserv])


    return Tools_with_reservations


def FAB300_with_tool_names(Tools_with_reservations,Tools_Parents):
    Expanded_Tools_parents = pd.merge(
        Tools_with_reservations, 
        Tools_Parents, 
        left_on=["FO_ROW_ID"], 
        right_on=["ROW_ID"], 
        how="left",
        suffixes=["","_y"]
    )
    Expanded_Tools_parents = Expanded_Tools_parents.rename(
        columns={
            "ENT_NAME":"Tool",
            "USER_ID_Start":"USER_ID_Begin",
            "USER_ID_End":"USER_ID_End"
        }
    )
    Expanded_Tools_parents_filteredRows = Expanded_Tools_parents[
        Expanded_Tools_parents["USER_ID_Begin"] == Expanded_Tools_parents["USER_ID_End"]
    ]
    Expanded_Tools_parents_filteredRows = Expanded_Tools_parents_filteredRows.rename(
        columns={
            "USER_ID_Begin":"User_id",
            "DATE_TIME_STAMP_Begin":"Begin",
            "DATE_TIME_STAMP_End":"End"
        }
    )
    Expanded_Tools_parents_filteredRows = Expanded_Tools_parents_filteredRows.sort_values(["FO_ROW_ID","EVENT_ROW_ID_Begin"])
    Fab300_Res_id = range(0,len(Expanded_Tools_parents_filteredRows))
    Expanded_Tools_parents_filteredRows["Fab300_Res_id"] = Fab300_Res_id
    Expanded_Tools_parents_filteredRows = Expanded_Tools_parents_filteredRows[
        (Expanded_Tools_parents_filteredRows["Tool"].notnull())
        & (Expanded_Tools_parents_filteredRows['Tool'].str.len() > 0)
    ]
    columns = [
        "FO_ROW_ID",
        "EVENT_ROW_ID_Begin",
        "Begin",
        "End",
        "Fab300_Res_id",
        "FACILITY",
        "ResTk",
        "Tool",
        "User_id",
        "WBS"
    ]
    Expanded_Tools_parents_filteredRows[columns]
    FAB300_with_tool_names = Expanded_Tools_parents_filteredRows[columns]

    #Sample for FO_ROW_ID == 76
    return FAB300_with_tool_names


#IIO_raw_reservations
def IIO_without_modules(df):
    df = df.reset_index()
    df["Begin"] = pd.to_datetime(df["Begin"],format="%d/%m/%Y %H:%M")
    df["End"] = pd.to_datetime(df["End"],format="%d/%m/%Y %H:%M")
    df = df.sort_values(["WBS","Tool","FACILITY","Module","Begin"])
    df["End_Down"] = df["End"].shift(1)
    df["Adjacent_Down"] = np.where(df['Begin']==df['End_Down'], True, False)
    df["IndexCopy"] = np.where(df["Adjacent_Down"] == True,np.nan,df["index"])
    df["IndexCopy2"] = df["IndexCopy"].fillna(method='ffill')
    df["Module"] = df["Module"].fillna("")
    df["Description"] = df["Description"].fillna("")
    df = df[['WBS','FACILITY', 'Module','Tool','Begin', 'Description', 'End', 'User_id','IndexCopy2','End_Down']]


    params = {
        'Begin': 'min',
        'End': 'max',
        'Description': lambda x: ';'.join(sorted(pd.Series.unique(x))),
        'User_id': lambda x: ';'.join(sorted(pd.Series.unique(x)))
    }
    sub = df[["WBS","Tool","FACILITY","Module","Begin","End","Description",'User_id','IndexCopy2']]
    GroupedRows1 = sub.groupby(["WBS","Tool","FACILITY","Module",'IndexCopy2']).agg(params).reset_index()

    params = {
        'Module': lambda x: ';'.join(sorted(pd.Series.unique(x))),
        'Description': 'first',
        'User_id': 'first'
    }
    

    GroupedRows1 = GroupedRows1.groupby(["WBS","Tool","FACILITY","Begin", "End"]).agg(params).reset_index()
    GroupedRows1 = GroupedRows1.sort_values(["FACILITY", "Tool", "WBS", "Begin", "End"])

    Index = range(0,len(GroupedRows1))
    GroupedRows1["IIO_Res_id"] = Index
    
    GroupedRows1 = GroupedRows1.rename(
        columns={
            "Module":"Modules"
        }
    )
    
    IIO_without_modules = GroupedRows1.copy()
    return IIO_without_modules


def Fab300_iio_merger(df):
    df["DateTime"] = pd.to_datetime(df["DateTime"])
    df = df.sort_values(["WBS","FACILITY",'Tool',"DateTime","FAB300_BeginEnd"])

    df["Fab300_Res_id_UP"] = df["Fab300_Res_id"].fillna(method='bfill')
    df["Fab300_Res_id_DOWN"] = df["Fab300_Res_id"].fillna(method='ffill')
    df["IIO_Res_id_UP"] = df["IIO_Res_id"].fillna(method='bfill')
    df["IIO_Res_id_DOWN"] = df["IIO_Res_id"].fillna(method='ffill')

    fab_iio_Filtered_Rows = df[
        (df["Fab300_Res_id_UP"] == df["Fab300_Res_id_DOWN"])
        & (df["IIO_Res_id_UP"] == df["IIO_Res_id_DOWN"])
        & (df["Fab300_Res_id_UP"].notnull())
        & (df["IIO_Res_id_UP"].notnull())
    ] 
    
    Removed_Other_Columns = fab_iio_Filtered_Rows[["Fab300_Res_id_UP", "IIO_Res_id_UP"]]
    Removed_Duplicates = Removed_Other_Columns.drop_duplicates()
    Renamed_Columns = Removed_Duplicates.rename(
        columns = {
            "Fab300_Res_id_UP":"Fab300_Res_id",
            "IIO_Res_id_UP":"IIO_Res_id"
        }
    )

    index = range(0,len(Renamed_Columns))
    Renamed_Columns["Index"] = index
    Added_Index = Renamed_Columns.copy()
    State_DOWN_Remove = Added_Index.copy()
    State_DOWN_Remove.loc[-1] = [np.nan,np.nan,-1]  # adding a row
    State_DOWN_Insert = State_DOWN_Remove.copy()
    State_DOWN_Insert = State_DOWN_Insert.sort_values(["Index"])
    index2 = range(0,len(State_DOWN_Insert))
    State_DOWN_Insert["Index2"] = index2

    State_DOWN_Add_Index = State_DOWN_Insert.copy()
    State_DOWN_Rename = State_DOWN_Add_Index.rename(
        columns = {
            "Fab300_Res_id":"Fab300_Res_id_DOWN",
            "IIO_Res_id":"IIO_Res_id_DOWN"
        }
    )

    With_DOWN = Added_Index.merge(
        State_DOWN_Rename, 
        left_on=["Index"],
        right_on=["Index2"],
        suffixes=["","_y"],
        how = 'left'
    )

    With_DOWN["Index3"] = np.where(
        (With_DOWN['Fab300_Res_id']==With_DOWN['Fab300_Res_id_DOWN'])
        | (With_DOWN['IIO_Res_id']==With_DOWN['IIO_Res_id_DOWN']), 
        np.nan,
         With_DOWN['Index']
    )


    Replaced_Value = With_DOWN.copy()
    Replaced_Value["Index4"] = Replaced_Value["Index3"].fillna(method='ffill')
    Replaced_Value = Replaced_Value.drop(columns=["Fab300_Res_id_DOWN", "IIO_Res_id_DOWN"])
    Renamed_Columns1 = Replaced_Value.rename(
    columns={
        "Index4":"Cluster"
    })


    Merged_queries = Renamed_Columns1.merge(
        Source_fab, 
        left_on=["Fab300_Res_id"],
        right_on=["Fab300_Res_id"],
        suffixes=["","_y"],
        how = 'left'
    )
    Expanded_Fab300_with_tool_names = Merged_queries.rename(
    columns ={
        "Begin": "Fab300_Begin", 
        "End": "Fab300_End", 
        "User_id": "Fab300_User_id"
    })
    Merged_queries_1 = Expanded_Fab300_with_tool_names.merge(
        Source_iio, 
        left_on=["IIO_Res_id"],
        right_on=["IIO_Res_id"],
        suffixes=["","_y"],
        how = 'left'
    )
    Expanded_IIO_without_modules = Merged_queries_1.rename(
    columns ={
        "Begin": "IIO_Begin", 
        "End": "IIO_End", 
        "Modules": "Modules",
        "User_id": "IIO_User_id",
        "Description": "Description"
    })
    Expanded_IIO_without_modules = Expanded_IIO_without_modules.drop(
        columns=["FACILITY_y","Modules","Tool_y","IIO_User_id","WBS_y",'Index', 'Index_y', 'Index2', 'Index3']
    )
    
    return Expanded_IIO_without_modules


def Final_Fab300_IIO_reservations(IIO_without_modules,Fab300withtoolnames,Fab300_IIO_overlaps_ids):
    Fab300_IIO_overlaps_ids["Fab300_Begin"] = pd.to_datetime(Fab300_IIO_overlaps_ids["Fab300_Begin"],format="%Y-%m-%d %H:%M")
    Fab300_IIO_overlaps_ids["Fab300_End"] = pd.to_datetime(Fab300_IIO_overlaps_ids["Fab300_End"],format="%Y-%m-%d %H:%M")
    Fab300_IIO_overlaps_ids["IIO_Begin"] = pd.to_datetime(Fab300_IIO_overlaps_ids["IIO_Begin"],format="%Y-%m-%d %H:%M")
    Fab300_IIO_overlaps_ids["IIO_End"] = pd.to_datetime(Fab300_IIO_overlaps_ids["IIO_End"],format="%Y-%m-%d %H:%M")
    
    AddedCustom = Fab300_IIO_overlaps_ids.copy()
    AddedCustom['Modules'] = AddedCustom['Modules'].fillna('')
    AddedCustom["Fab300_Duration"] = (AddedCustom["Fab300_End"] - AddedCustom["Fab300_Begin"])/np.timedelta64(1, 'h')
    AddedCustom1 = AddedCustom.copy()
    AddedCustom1["IIO_Duration"] = (AddedCustom1["IIO_End"] - AddedCustom1["IIO_Begin"])/np.timedelta64(1, 'h')
    params = {
        'Fab300_Begin': 'min',
        'IIO_Begin': 'min',
        'Fab300_End': 'max',
        'IIO_End': 'max',
        'Fab300_Res_id': 'count',
        'Description': lambda x: ';'.join(sorted(pd.Series.unique(x))),
        'IIO_User_id': lambda x: ';'.join(sorted(pd.Series.unique(x))),
        'Fab300_User_id': lambda x: ';'.join(sorted(pd.Series.unique(x))),
        'Modules': lambda x: ';'.join(sorted(pd.Series.unique(x)))
    }
    sub = AddedCustom1[
        [
            "FACILITY", 
            "Tool", 
            "WBS", 
            "Cluster",
            "Fab300_Begin",
            "IIO_Begin",
            'Fab300_End',
            'IIO_End',
            'Fab300_Res_id',
            'IIO_Res_id',
            'Description',
            'IIO_User_id',
            'Fab300_User_id',
            'Modules'
        ]
    ]
    GroupedRows1 = sub.groupby(["FACILITY", "Tool", "WBS", "Cluster"]).agg(params).reset_index()
    GroupedRows1["Begin"] = np.where(GroupedRows1['Fab300_Begin']>GroupedRows1['IIO_Begin'], GroupedRows1['IIO_Begin'], GroupedRows1['Fab300_Begin'])
    GroupedRows1["End"]  = np.where(GroupedRows1['Fab300_End']>GroupedRows1['IIO_End'], GroupedRows1['Fab300_End'], GroupedRows1['IIO_End'])
    GroupedRows1 = GroupedRows1.rename(
    columns={
        "Fab300_Res_id": "Cnt"
    }).drop(columns=['Fab300_Begin','Fab300_End','IIO_Begin','IIO_End'])


    Duration_FAB = AddedCustom1[
        [
            "FACILITY", 
            "Tool", 
            "WBS", 
            "Cluster",
            "Fab300_Res_id",
            'Fab300_Duration'
        ]
    ].drop_duplicates().reset_index()

    Duration_IIO = AddedCustom1[
        [
            "FACILITY", 
            "Tool", 
            "WBS", 
            "Cluster",
            "IIO_Res_id",
            'IIO_Duration'
        ]
    ].drop_duplicates().reset_index()

    GroupedRows1_FAB = Duration_FAB.groupby(["FACILITY", "Tool", "WBS", "Cluster"]).sum().reset_index()
    GroupedRows1_IIO = Duration_IIO.groupby(["FACILITY", "Tool", "WBS", "Cluster"]).sum().reset_index()


    GroupedRows = pd.concat(
        objs=(iDF.set_index(["FACILITY", "Tool", "WBS", "Cluster"]) for iDF in (GroupedRows1, GroupedRows1_FAB, GroupedRows1_IIO)),
        axis=1, 
        join='inner'
    ).reset_index()
    GroupedRows['FAB_IIO_Ratio'] = GroupedRows["Fab300_Duration"] / GroupedRows["IIO_Duration"]

    GroupedRows = GroupedRows[
        [
            'FACILITY', 
            'Tool', 
            'WBS', 
            'Cluster', 
            'Cnt', 
            'Description',
            'Begin', 
            'End',
            'IIO_User_id', 
            'Fab300_User_id', 
            'Modules',
            'Fab300_Duration', 
            'IIO_Duration', 
            'FAB_IIO_Ratio'
        ]
    ]

    FilteredRows = GroupedRows[
        (GroupedRows["FAB_IIO_Ratio"] < 3.0)
        & (GroupedRows["Cnt"] <= 4)
    ]

    Fab300_IIO_valid_combos = FilteredRows.drop(columns = ["Cnt"])

    outer = Fab300_IIO_overlaps_ids.merge(
        Fab300_IIO_valid_combos, 
        how='outer',
        left_on = ["FACILITY", "Tool", "WBS", "Cluster"],
        right_on = ["FACILITY", "Tool", "WBS", "Cluster"],
        indicator=True
    )
    Fab300_IIO_bad_combos = outer[(outer._merge=='left_only')].drop('_merge', axis=1)


    IIO_ids_from_bad_combos = Fab300_IIO_bad_combos[["IIO_Res_id"]]
    RemovedDuplicates = IIO_ids_from_bad_combos.drop_duplicates()

    MergedQueries = RemovedDuplicates.merge(
        IIO_without_modules, 
        how='inner',
        left_on = ["IIO_Res_id"],
        right_on = ["IIO_Res_id"],
        indicator=True
    )
    RemovedColumns1 = MergedQueries.drop(columns=["IIO_Res_id"])
    IIO_from_bad_combos = RemovedColumns1.rename(
    columns = {
        'User_id':'IIO_User_id'
    }).drop(columns=["_merge"])

    outer_IIO_from_bad_combos = IIO_without_modules.merge(
        Fab300_IIO_overlaps_ids, 
        how='outer',
        left_on = ["IIO_Res_id"],
        right_on = ["IIO_Res_id"],
        indicator=True
    )
    Pure_IIO_Join = outer_IIO_from_bad_combos[(outer_IIO_from_bad_combos._merge=='left_only')].drop('_merge', axis=1)
    Pure_IIO = Pure_IIO_Join[
        [
            'Begin', 
            'Description_x', 
            'End', 
            'FACILITY_x', 
            'IIO_Res_id',
            'Modules_x', 
            'Tool_x', 
            'User_id', 
            'WBS_x'
        ]
    ]
    RenamedColumns = Pure_IIO.rename(
        columns = {
            'User_id':'IIO_User_id',
            'Description_x': 'Description',
            'FACILITY_x': 'FACILITY',
            'Modules_x': 'Modules',
            'Tool_x': 'Tool',
            'WBS_x': 'WBS'
        }
    )
    AppendedQuery = pd.concat([RenamedColumns,IIO_from_bad_combos]).drop(columns=['IIO_Res_id'])
    AppendedQuery["Begin"] = pd.to_datetime(AppendedQuery["Begin"],format="%Y-%m-%d %H:%M")
    AppendedQuery["End"] = pd.to_datetime(AppendedQuery["End"],format="%Y-%m-%d %H:%M")
    AppendedQuery["IIO_Duration"] = (AppendedQuery["End"] - AppendedQuery["Begin"])/np.timedelta64(1, 'h')
    AddedCustom2 = AppendedQuery.copy()
    AppendedQuery2 = pd.concat([Fab300_IIO_valid_combos,AddedCustom2])

    outer_Fab300withtoolnames = Fab300withtoolnames.merge(
        Fab300_IIO_overlaps_ids, 
        how='outer',
        left_on = ["Fab300_Res_id"],
        right_on = ["Fab300_Res_id"],
        indicator=True
    )

    Pure_Fab300_Join = outer_Fab300withtoolnames[(outer_Fab300withtoolnames._merge=='left_only')].drop('_merge', axis=1)
    Pure_Fab300 = Pure_Fab300_Join[
        [
            'Begin', 
            'End', 
            'Fab300_Res_id', 
            'FACILITY_x', 
            'Tool_x',
            'User_id',
            'WBS_x'
        ]
    ].drop(columns=['Fab300_Res_id']).rename(
    columns={
            'FACILITY_x': 'FACILITY',
            'Tool_x': 'Tool',
            'User_id': 'Fab300_User_id',
            'WBS_x': 'WBS'
    })
    RenamedColumns1 = Pure_Fab300.copy()

    RenamedColumns1["Begin"] = pd.to_datetime(RenamedColumns1["Begin"],format="%Y-%m-%d %H:%M")
    RenamedColumns1["End"] = pd.to_datetime(RenamedColumns1["End"],format="%Y-%m-%d %H:%M")

    RenamedColumns1["Fab300_Duration"] = (RenamedColumns1["End"] - RenamedColumns1["Begin"])/np.timedelta64(1, 'h')
    AddedCustom3 = RenamedColumns1.copy()

    AppendedQuery3 = pd.concat([AppendedQuery2,AddedCustom3]).drop(columns=['FAB_IIO_Ratio'])
    return AppendedQuery3



#Start of execution
df = pd.read_csv("Fab300_raw_reservations.csv")
Tools_with_reservations = processFab300RawReservations(df)

Tools_Parents = pd.read_csv("Tools_Parents.csv")
Tools_Parents["CSIM_TIMESTAMP"] = pd.to_datetime(Tools_Parents["CSIM_TIMESTAMP"])

df_FAB300_with_tool_names = FAB300_with_tool_names(Tools_with_reservations,Tools_Parents)


df = pd.read_csv("IIO_raw_reservations - test.csv")
df_IIO_without_modules = IIO_without_modules(df)


#Fab300_IIO_overlaps_ids
Source_fab = df_FAB300_with_tool_names.copy()
RC_fab = Source_fab.drop(columns=["ResTk", "User_id"])
UnPivot_fab = pd.melt(RC_fab, id_vars=["WBS", "FACILITY", "Tool", "Fab300_Res_id"], 
                value_vars=["Begin", "End"])
UnPivot_fab = UnPivot_fab.rename(
columns={
    "variable": "FAB300_BeginEnd",
    "value": "DateTime"
})

Source_iio = df_IIO_without_modules.copy()
RC_iio = Source_iio.drop(columns=["Modules", "User_id", "Description"])
UnPivot_iio = pd.melt(RC_iio, id_vars=["WBS", "FACILITY", "Tool", "IIO_Res_id"], 
                value_vars=["Begin", "End"])

UnPivot_iio = UnPivot_iio.rename(
columns={
    "variable": "IIO_BeginEnd",
    "value": "DateTime"
})

fab_iio_together  = pd.concat([UnPivot_fab, UnPivot_iio], ignore_index=True)


columns = [
    'Fab300_Res_id', 
    'IIO_Res_id', 
    'Cluster', 
    'Fab300_Begin', 
    'Fab300_End',
    'FACILITY', 
    'ResTk', 
    'Tool', 
    'Fab300_User_id', 
    'WBS', 
    'IIO_Begin',
    'Description', 
    'IIO_End'
]

Fab300_IIO_overlaps_ids = pd.DataFrame(columns = columns)

WBSs = fab_iio_together["WBS"].unique()

for wbs in WBSs:
    wbsdata = fab_iio_together[
        (fab_iio_together["WBS"] == wbs)
    ]
    facilities = wbsdata["FACILITY"].unique()
    for facility in facilities:
        facilityData = wbsdata[
            (wbsdata["FACILITY"] == facility)
        ]
        
        tools = facilityData["Tool"].unique()
        for tool in tools:
            wbstooldata = facilityData[
                (wbsdata["Tool"] == tool)
            ]
            df = Fab300_iio_merger(wbstooldata)
            Fab300_IIO_overlaps_ids = pd.concat([Fab300_IIO_overlaps_ids,df])
            
            

#Final_Fab300_IIO_reservations
Final_Fab300_IIO_reservations_df = Final_Fab300_IIO_reservations(df_IIO_without_modules,df_FAB300_with_tool_names,Fab300_IIO_overlaps_ids)

print(Final_Fab300_IIO_reservations_df.head())
