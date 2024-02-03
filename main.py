try:
    import time
    import os
    import sys
    import getpass
    import pandas as pd
    import xml.etree.ElementTree as ETree
    import numpy as np
    from utils.logger import logger_init
    from utils.Common_Functions_64 import removeExtraDelimiter, digit_to_nondigit, split_into_rows, ExpandSeries, delete_file

except ImportError as IE:
    print(f"Import Error: {str(IE)}")
    time.sleep(5)


def init():
    '''init'''

    # Get path_main and transform into absolute path (so it works for onedrive path too)
    sharepoint_online_path = f"https://microncorp-my.sharepoint.com/personal/{getpass.getuser()}_micron.com/Documents/"
    sharepoint_local_path = f"C:\\Users\\{getpass.getuser()}\\OneDrive - Micron Technology, Inc\\"
    path_main = os.path.dirname(os.path.realpath(sys.argv[0]))
    path_main = path_main.replace(sharepoint_online_path, sharepoint_local_path)
    path_main = path_main.replace("/", "\\")

    # Define working folder paths
    path_590 = f"{path_main}\\BOM_590"
    path_MCTO = f"{path_main}\\MCTO"
    path_recipe_bom_master = f"{path_main}\\recipe-bom-master"
    path_recipe_bom_new = f"{path_main}\\recipe-bom-new"
    filename_bom = 'BOM.xlsx'
    path_bom = f"{path_main}\\{filename_bom}"

    # Init logger
    try:
        df_settings = pd.read_excel(path_bom, sheet_name='settings')
        loglevel = list(df_settings['LOG_LEVEL'])[0]
        loglevel_error = False 
    except (ValueError, KeyError):
        loglevel_error = True
        loglevel = 'INFO'

    if loglevel_error:
        log.warning('LOG_LEVEL is not defined in settings, setting to INFO...')

    log = logger_init('BOM_PROGRAM_CREATE.log', f"{path_main}\\Log", 'w', loglevel)
    log.info(f"Running main.py in {path_main} with loglevel = {loglevel}")

    return log, path_main, path_590, path_MCTO, path_recipe_bom_master, path_recipe_bom_new, path_bom


def main(log, path_main, path_590, path_MCTO, path_recipe_bom_master, path_recipe_bom_new, path_bom):
    '''main'''
    
    log.info(f"path_590 = {path_590}")
    log.info(f"path_MCTO = {path_MCTO}")
    log.info(f"path_recipe_bom_master = {path_recipe_bom_master}")
    log.info(f"path_recipe_bom_new = {path_recipe_bom_new}")
    log.info(f"path_bom = {path_bom}")

    try:
        df_settings = pd.read_excel(path_bom, sheet_name='settings')
        SAP_SOURCE = list(df_settings['SAP_SOURCE'])[0]
    except (ValueError, KeyError):
        log.warning('SAP_SOURCE is not defined in settings, setting to manual...')
        SAP_SOURCE = 'manual'

    log.info(f"SAP_SOURCE = {SAP_SOURCE}")

    exclude_comp_prefix = ('590', '550', '540', '542', '561', '562', 'ECN')
    log.info(f"exclude_comp_prefix = {exclude_comp_prefix}")

    # Read main excel workbook
    log.info('Reading bom file...')
    input_columns = ['BOM_MASTER', 'MCTO_MASTER', 'PV_MASTER', 'PNP_PROGRAM_SIDE1_MASTER', 'PNP_PROGRAM_SIDE2_MASTER', 'BOM_NEW', 'MCTO_NEW', 'PV_NEW', 'PNP_PROGRAM_SIDE1_NEW', 'PNP_PROGRAM_SIDE2_NEW']

    # Create df_input for input sheet
    log.info('Creating dataframe for input sheet...')
    df_input = pd.read_excel(path_bom, sheet_name='BOM')
    log.debug(f"\n{df_input.head(5).to_string(index=False)}")

    log.debug('Dropping null rows...')
    df_input.dropna(how='any', subset=['BOM_MASTER', 'MCTO_MASTER', 'PV_MASTER', 'PNP_PROGRAM_SIDE1_MASTER', 'BOM_NEW', 'MCTO_NEW', 'PV_NEW', 'PNP_PROGRAM_SIDE1_NEW'], inplace=True)
    log.debug(f"\n{df_input.head(5).to_string(index=False)}")

    if len(df_input) < 1:
        raise ConnectionAbortedError ('There is no input to be processed, force exiting application...')

    log.debug('Converting all input columns to str...')
    df_input = df_input[input_columns].astype(str)
    log.debug(f"\n{df_input.head(5).to_string(index=False)}")

    log.debug('Trimming all input columns...')
    for input_column in input_columns:
        df_input[input_column] = df_input[input_column].str.strip().str.upper()
    log.debug(f"\n{df_input.head(5).to_string(index=False)}")

    log.debug('Dropping duplicates...')
    df_input.drop_duplicates(subset=['BOM_MASTER', 'MCTO_MASTER', 'PV_MASTER', 'PNP_PROGRAM_SIDE1_MASTER', 'PNP_PROGRAM_SIDE2_MASTER', 'BOM_NEW', 'MCTO_NEW', 'PV_NEW', 'PNP_PROGRAM_SIDE1_NEW', 'PNP_PROGRAM_SIDE2_NEW'], keep='first', inplace=True)
    log.debug(f"\n{df_input.head(5).to_string(index=False)}")

    log.debug('Removing duplicates of selected 590 and MCTO...')
    selected_590 = set(df_input['BOM_MASTER']).union(set(df_input['BOM_NEW']))
    selected_MCTO = set(df_input['MCTO_MASTER'] + '_' + df_input['PV_MASTER']).union(set(df_input['MCTO_NEW'] + '_' + df_input['PV_NEW']))
    log.info(f"Selected_590 = {selected_590}")
    log.info(f"Selected_MCTO = {selected_MCTO}")

    if SAP_SOURCE == 'manual':
        log.info('Scanning files_590 and files_MCTO...')
        scan_files_590 = os.scandir(path_590)
        scan_files_MCTO = os.scandir(path_MCTO)

        file_590 = {f.path for f in scan_files_590 if f.name[-4:].lower() == '.csv' and any (matcher in f.name for matcher in selected_590)}
        file_MCTO = {f.path for f in scan_files_MCTO if f.name[-4:].lower() == '.csv' and any (matcher in f.name for matcher in selected_MCTO)}

        log.info(f"Matched file_590 = {file_590}")
        log.info(f"Matched file_MCTO = {file_MCTO}")

        # Raise warning if no 590 or MCTO file is found
        if len(file_590) < 1 or len(file_MCTO) < 1:
            raise ConnectionAbortedError ('There is no selected 590 or MCTO file found, force exiting application...')

        # Combine all files_590
        log.info(f"Starting to read {str(len(file_590))} BOM_590 files...")
        df_590 = pd.DataFrame()
        for file in file_590:
            log.info(f"Reading: {file}...")
            df = pd.read_csv(file, sep='\t', skiprows=9, usecols=[1,3,5,10], skip_blank_lines=True, skipinitialspace=True, on_bad_lines='warn')
            df.columns = df.columns.str.strip()
            log.debug(f"\n{df.head(5).to_string(index=False)}")

            log.debug('Renaming columns...')
            df = df.rename(columns={'Object no.':'COMPONENT', 'Quantity':'QUANTITY', 'Material Description':'COMPDESC', 'Reference Designator':'DESIGNATOR'})
            log.debug(f"\n{df.head(5).to_string(index=False)}")

            log.debug('Trimming all input columns...')
            input_columns = ['COMPONENT', 'QUANTITY', 'COMPDESC', 'DESIGNATOR']
            for input_column in input_columns:
                df[input_column] = df[input_column].str.strip().str.upper()
            log.debug(f"\n{df.head(5).to_string(index=False)}")

            log.debug('Create BOM column with component starting with 590...')
            df['BOM'] = np.where(df['COMPONENT'].str.startswith('590'), df['COMPONENT'], np.NaN)
            log.debug(f"\n{df.head(5).to_string(index=False)}")

            log.debug('Front-filling BOM...')
            df['BOM'].ffill(inplace=True)
            log.debug(f"\n{df.head(5).to_string(index=False)}")

            log.debug('Replacing empty with NaN...')
            df = df.replace([' '], ['']).replace([''], [np.NaN], regex=True)
            log.debug(f"\n{df.head(5).to_string(index=False)}")

            log.debug('Removing rows with null designator...')
            df = df[~df.DESIGNATOR.isnull()]
            log.debug(f"\n{df.head(5).to_string(index=False)}")

            log.debug('Front-filling...')
            df.ffill(inplace=True)
            log.debug(f"\n{df.head(5).to_string(index=False)}")

            log.debug(f"Removing rows with comp_prefix = {exclude_comp_prefix}...")
            df = df[~df.COMPONENT.str.startswith(exclude_comp_prefix)]
            log.debug(f"\n{df.head(5).to_string(index=False)}")

            log.debug('Removing rows with comp_prefox = 511 and compdesc contains TH AE or THAE...')
            df = df[~(df.COMPONENT.str.startswith('511') & (df.COMPDESC.str.contains('TH AE') | df.COMPDESC.str.contains('THAE')))]
            log.debug(f"\n{df.head(5).to_string(index=False)}")

            log.debug('Converting quantity string into int...')
            df['QUANTITY'] = df['QUANTITY'].replace([','], ['.'], regex=True)
            df['QUANTITY'] = df['QUANTITY'].str.split('.').str[0].str.strip()
            df['QUANTITY'] = df['QUANTITY'].fillna('0').astype(int)
            log.debug(f"\n{df.head(5).to_string(index=False)}")

            log.debug('Stripping all string columns...')
            df['BOM'] = df['BOM'].str.strip()
            df['COMPONENT'] = df['COMPONENT'].str.strip()
            df['COMPDESC'] = df['COMPDESC'].str.strip()
            df['DESIGNATOR'] = df['DESIGNATOR'].str.strip()
            df = df[['BOM', 'COMPONENT', 'COMPDESC', 'QUANTITY', 'DESIGNATOR']]
            log.debug(f"\n{df.head(5).to_string(index=False)}")

            log.debug('Grouping designator...')
            df = df.groupby(['BOM', 'COMPONENT', 'COMPDESC', 'QUANTITY'])['DESIGNATOR'].apply(','.join).reset_index()
            log.debug(f"\n{df.head(5).to_string(index=False)}")

            log.debug('Removing extra delimiter from designator...')
            df['DESIGNATOR'] = df['DESIGNATOR'].apply(removeExtraDelimiter)
            log.debug(f"\n{df.head(5).to_string(index=False)}")

            log.debug('Expanding designator series...')
            df['DESIGNATOR'] = df['DESIGNATOR'].apply(ExpandSeries)
            log.debug(f"\n{df.head(5).to_string(index=False)}")

            if df['DESIGNATOR'].str.contains('-').any():
                log.warning(f"Designators are not expanded, skipping {file}...")
                continue

            df['QUANTITY'] = 1

            log.debug('Dropping duplicates...')
            df.drop_duplicates(subset=['BOM', 'COMPONENT', 'COMPDESC', 'QUANTITY', 'DESIGNATOR'], keep='last', inplace=True)
            log.debug(f"\n{df.head(5).to_string(index=False)}")

            log.debug(f"Concating {str(len(df))} rows into df_590...")
            df_590 = pd.concat([df_590, df], ignore_index=True)
            log.debug(f"\n{df.head(5).to_string(index=False)}")
        log.info(f"Total of {str(len(df_590))} rows detected in BOM_590 files.")

        # Combine all MCTO files
        log.info(f"Starting to read {str(len(file_MCTO))} MCTO files...")
        df_MCTO = pd.DataFrame()
        for file in file_MCTO:
            filename = file.rsplit('\\', 1)[-1]
            filename_without_ext = filename.rsplit('.', 1)[0]
            try:
                PV = filename_without_ext.rsplit('_', 1)[1]
            except IndexError:
                PV = '1'

            log.info(f"Reading: {file}...")
            df = pd.read_csv(file, sep='\t', skiprows=9, usecols=[1,4,6,11], skip_blank_lines=True, skipinitialspace=True, on_bad_lines='warn')
            df.columns = df.columns.str.strip()
            log.debug(f"\n{df.head(5).to_string(index=False)}")

            log.debug('Renaming columns...')
            df = df.rename(columns={'Object no.':'COMPONENT', 'Quantity':'QUANTITY', 'Material Description':'COMPDESC', 'Reference Designator':'DESIGNATOR'})
            log.debug(f"\n{df.head(5).to_string(index=False)}")

            log.debug('Trimming all input columns...')
            input_columns = ['COMPONENT', 'QUANTITY', 'COMPDESC', 'DESIGNATOR']
            for input_column in input_columns:
                df[input_column] = df[input_column].str.strip().str.upper()
            log.debug(f"\n{df.head(5).to_string(index=False)}")

            log.debug('Create MCTO column with component not null and compdesc, quantity, designator are null...')
            df['MCTO'] = np.where(~(df['COMPONENT'].isna()) & (df['COMPDESC'].isnull()) & (df['QUANTITY'].isnull()) & (df['DESIGNATOR'].isnull()), df['COMPONENT'], np.NaN)
            log.debug(f"\n{df.head(5).to_string(index=False)}")

            log.debug('Front-filling MCTO...')
            df['MCTO'].ffill(inplace=True)
            log.debug(f"\n{df.head(5).to_string(index=False)}")

            log.debug('Adding PV columns...')
            df['PV'] = PV
            log.debug(f"\n{df.head(5).to_string(index=False)}")

            log.debug('Replacing empty with NaN...')
            df = df.replace([' '], ['']).replace([''], [np.NaN], regex=True)
            log.debug(f"\n{df.head(5).to_string(index=False)}")

            log.debug('Removing rows with null designator...')
            df = df[~df.DESIGNATOR.isnull()]
            log.debug(f"\n{df.head(5).to_string(index=False)}")

            log.debug('Front-filling...')
            df.ffill(inplace=True)
            log.debug(f"\n{df.head(5).to_string(index=False)}")

            log.debug(f"Removing rows with comp_prefix = {exclude_comp_prefix}...")
            df = df[~df.COMPONENT.str.startswith(exclude_comp_prefix)]
            log.debug(f"\n{df.head(5).to_string(index=False)}")

            log.debug('Converting quantity string into int...')
            df['QUANTITY'] = df['QUANTITY'].replace([','], ['.'], regex=True)
            df['QUANTITY'] = df['QUANTITY'].str.split('.').str[0].str.strip()
            df['QUANTITY'] = df['QUANTITY'].fillna('0').astype(int)
            log.debug(f"\n{df.head(5).to_string(index=False)}")

            log.debug('Stripping all string columns...')
            df['MCTO'] = df['MCTO'].str.strip()
            df['COMPONENT'] = df['COMPONENT'].str.strip()
            df['COMPDESC'] = df['COMPDESC'].str.strip()
            df['DESIGNATOR'] = df['DESIGNATOR'].str.strip()
            df = df[['MCTO', 'PV', 'COMPONENT', 'COMPDESC', 'QUANTITY', 'DESIGNATOR']]
            log.debug(f"\n{df.head(5).to_string(index=False)}")

            log.debug('Grouping designator...')
            df = df.groupby(['MCTO', 'PV', 'COMPONENT', 'COMPDESC', 'QUANTITY'])['DESIGNATOR'].apply(','.join).reset_index()
            log.debug(f"\n{df.head(5).to_string(index=False)}")

            log.debug('Removing extra delimiter from designator...')
            df['DESIGNATOR'] = df['DESIGNATOR'].apply(removeExtraDelimiter)
            log.debug(f"\n{df.head(5).to_string(index=False)}")

            log.debug('Expanding designator series...')
            df['DESIGNATOR'] = df['DESIGNATOR'].apply(ExpandSeries)
            log.debug(f"\n{df.head(5).to_string(index=False)}")

            if df['DESIGNATOR'].str.contains('-').any():
                log.warning(f"Designators are not expanded, skipping {file}...")
                continue

            df['QUANTITY'] = 1

            log.debug('Dropping duplicates...')
            df.drop_duplicates(subset=['MCTO', 'PV', 'COMPONENT', 'COMPDESC', 'QUANTITY', 'DESIGNATOR'], keep='last', inplace=True)
            log.debug(f"\n{df.head(5).to_string(index=False)}")

            log.debug(f"Concating {str(len(df))} rows into df_MCTO...")
            df_MCTO = pd.concat([df_MCTO, df], ignore_index=True)
            log.debug(f"\n{df.head(5).to_string(index=False)}")
        log.info(f"Total of {str(len(df_MCTO))} rows detected in BOM_MCTO files.")

    else:
        log.info('Loading SAP database...')
        from settings import DB_TYPE
        from db_connection import connect_db
        connection, conn_response = connect_db(db_type=DB_TYPE)

        if connection is None:
            raise ConnectionAbortedError(conn_response)
        
        log.info(conn_response)
        
        query_BOM_590 = '''
                    SELECT BOM, COMPONENT, COMPDESC, QUANTITY, DESIGNATOR, 'BOM_590' as 'GROUP' FROM [localdb].[dbo].[BOM_590];
                '''
        df_590 = pd.read_sql(sql=query_BOM_590, con=connection)

        log.debug('Expanding designator series...')
        df_590['DESIGNATOR'] = df_590['DESIGNATOR'].apply(ExpandSeries)

        if df_590['DESIGNATOR'].str.contains('-').any():
            raise ConnectionAbortedError ('Designators are not expanded, force exiting application...')

        df_590['QUANTITY'] = 1
        log.debug(f"\n{df_590.head(5).to_string(index=False)}")

        query_MCTO = '''
                    SELECT MCTO, PV, COMPONENT, COMPDESC, QUANTITY, DESIGNATOR, 'MCTO' as 'GROUP' FROM [localdb].[dbo].[MCTO];
                '''
        df_MCTO = pd.read_sql(sql=query_MCTO, con=connection)

        log.debug('Expanding designator series...')
        df_MCTO['DESIGNATOR'] = df_MCTO['DESIGNATOR'].apply(ExpandSeries)

        if df_MCTO['DESIGNATOR'].str.contains('-').any():
            raise ConnectionAbortedError ('Designators are not expanded, force exiting application...')
        
        df_MCTO['QUANTITY'] = 1
        log.debug(f"\n{df_MCTO.head(5).to_string(index=False)}")

        if connection is not None:
            connection.close()

    log.info('Merging 590 and MCTO into df_590_MCTO_all...')

    log.debug('Creating df_590_MCTO_PV dataframe...')
    df_590_MCTO_PV_master = df_input[['BOM_MASTER', 'MCTO_MASTER', 'PV_MASTER']].rename(columns={'BOM_MASTER':'BOM', 'MCTO_MASTER':'MCTO', 'PV_MASTER':'PV'})
    df_590_MCTO_PV_new = df_input[['BOM_NEW', 'MCTO_NEW', 'PV_NEW']].rename(columns={'BOM_NEW':'BOM', 'MCTO_NEW':'MCTO', 'PV_NEW':'PV'})
    df_590_MCTO_PV = pd.concat([df_590_MCTO_PV_master, df_590_MCTO_PV_new], ignore_index=True)
    df_590_MCTO_PV.drop_duplicates(subset=['BOM', 'MCTO', 'PV'], keep='last', inplace=True)
    log.debug(f"\n{df_590_MCTO_PV.head(5).to_string(index=False)}")

    log.debug('Left joining df_590 to df_590_MCTO_PV on BOM and drop duplicates...')
    df_590_all = df_590.merge(df_590_MCTO_PV, how='inner', left_on='BOM', right_on='BOM')
    df_590_all['GROUP'] = 'BOM_590' 
    df_590_all = df_590_all[['BOM', 'MCTO', 'PV', 'COMPONENT', 'COMPDESC', 'QUANTITY', 'DESIGNATOR', 'GROUP']]
    df_590_all.drop_duplicates(subset=['BOM', 'MCTO', 'PV', 'COMPONENT', 'COMPDESC', 'QUANTITY', 'DESIGNATOR', 'GROUP'], keep='last', inplace=True)
    log.debug(f"\n{df_590_all.head(5).to_string(index=False)}")

    log.debug('Left joining df_MCTO to df_590_MCTO_PV on MCTO and drop duplicates...')
    df_MCTO_all = df_MCTO.merge(df_590_MCTO_PV, how='inner', left_on=['MCTO', 'PV'], right_on=['MCTO', 'PV'])
    df_MCTO_all['GROUP'] = 'MCTO'
    df_MCTO_all = df_MCTO_all[['BOM', 'MCTO', 'PV', 'COMPONENT', 'COMPDESC', 'QUANTITY', 'DESIGNATOR', 'GROUP']]
    df_MCTO_all.drop_duplicates(subset=['BOM', 'MCTO', 'PV', 'COMPONENT', 'COMPDESC', 'QUANTITY', 'DESIGNATOR', 'GROUP'], keep='last', inplace=True)
    log.debug(f"\n{df_MCTO_all.head(5).to_string(index=False)}")

    # Check if 590/MCTO is having any data
    if len(df_590_all) < 1 or len(df_MCTO_all) < 1:
        raise ConnectionAbortedError (f"df_590_all or df_MCTO_all is empty, force exiting application...")

    log.debug('Concating df_590_all into df_MCTO_all...')
    df_590_MCTO_all = pd.concat([df_590_all, df_MCTO_all], ignore_index=True)
    log.debug(f"\n{df_590_MCTO_all.head(5).to_string(index=False)}")
    log.info(f"Total of {str(len(df_590_MCTO_all))} rows detected in df_590_MCTO_all")

    log.info('Running logic to convert memory component into 520-XXX ...')
    df_Material = df_590_MCTO_all
    df_Material['MemoryDesc'] = np.where(~df_Material['COMPONENT'].str.contains('-'), df_Material['COMPDESC'], np.NaN)
    df_Material['MemoryDesc_dash'] = df_Material['MemoryDesc'].str.split('-').str[0].str.strip()
    df_Material['MemoryDesc_last'] = df_Material['MemoryDesc_dash'].str[-3:]
    df_Material['Last1'] = df_Material['MemoryDesc_last'].apply(digit_to_nondigit, keep='First').fillna('').replace('nan', '', regex=True).astype(str)
    df_Material['Last2'] = df_Material['MemoryDesc_last'].apply(digit_to_nondigit, keep='Last').fillna('').replace('nan', '', regex=True).astype(str)
    df_Material['MemoryDesc_:'] = df_Material['MemoryDesc'].str.split(':').str[-2]
    df_Material['COMPONENT2'] = np.where(df_Material['COMPONENT'].str.contains('-'), df_Material['COMPONENT'], np.where(df_Material['COMPDESC'].str.startswith('MTC'), df_Material['MemoryDesc_:'], np.where(df_Material['Last2'] != '', '520-' + df_Material['Last2'], np.where(df_Material['COMPDESC'].str.startswith('MT2'), '520-' + df_Material['Last1'].str[-2:], '520-' + df_Material['Last1']))))
    df_Material = df_Material.rename(columns={'COMPONENT':'COMPONENT3', 'COMPONENT2':'COMPONENT'})
    df_Material = df_Material[['BOM', 'MCTO', 'PV', 'COMPONENT', 'COMPDESC', 'QUANTITY', 'DESIGNATOR', 'GROUP']]
    df_Material.drop_duplicates(subset=['BOM', 'MCTO', 'PV', 'COMPONENT', 'COMPDESC', 'QUANTITY', 'DESIGNATOR', 'GROUP'], keep='last', inplace=True)
    df_Material = df_Material.sort_values(by=['BOM', 'MCTO', 'PV', 'COMPONENT', 'DESIGNATOR'])
    log.debug(f"\n{df_Material.head(5).to_string(index=False)}")

    log.info(f"Total of {str(len(df_Material))} rows detected in df_Material")

    log.info('Starting to loop through df_input...')
    for i in range(len(df_input)):

        try:

            info_master = (df_input.loc[i, 'BOM_MASTER'], df_input.loc[i, 'MCTO_MASTER'], df_input.loc[i, 'PV_MASTER'])
            info_new = (df_input.loc[i, 'BOM_NEW'], df_input.loc[i, 'MCTO_NEW'], df_input.loc[i, 'PV_NEW'])
            log.info(f"Reading row #{i+1} with (BOM_MASTER, MCTO_MASTER, PV_MASTER) = {info_master} and (BOM_NEW, MCTO_NEW, PV_NEW) = {info_new} ...")
            log.debug(f"\n{df_input.take([i]).to_string(index=False)}")

            log.info('Comparing master and new...')

            log.debug('Creating df_master...')
            df_master_590 = df_Material.loc[(df_Material['BOM'] == info_master[0]) & (df_Material['MCTO'] == info_master[1]) & (df_Material['PV'] == info_master[2]) & (df_Material['GROUP'] == 'BOM_590')]
            df_master_MCTO = df_Material.loc[(df_Material['BOM'] == info_master[0]) & (df_Material['MCTO'] == info_master[1]) & (df_Material['PV'] == info_master[2]) & (df_Material['GROUP'] == 'MCTO')]
            df_master = pd.concat([df_master_590, df_master_MCTO], ignore_index=True)
            log.debug(f"\n{df_master.head(5).to_string(index=False)}")

            log.debug('Creating df_new...')
            df_new_590 = df_Material.loc[(df_Material['BOM'] == info_new[0]) & (df_Material['MCTO'] == info_new[1]) & (df_Material['PV'] == info_new[2]) & (df_Material['GROUP'] == 'BOM_590')]
            df_new_MCTO = df_Material.loc[(df_Material['BOM'] == info_new[0]) & (df_Material['MCTO'] == info_new[1]) & (df_Material['PV'] == info_new[2]) & (df_Material['GROUP'] == 'MCTO')]
            df_new = pd.concat([df_new_590, df_new_MCTO], ignore_index=True)
            log.debug(f"\n{df_new.head(5).to_string(index=False)}")

            if len(df_master_590) < 1 or len(df_new_590) < 1 or len(df_master_MCTO) < 1 or len(df_new_MCTO) < 1:
                raise AssertionError (f"Master BOM or New BOM is empty, skipping row #{i+1} (BOM_NEW, MCTO_NEW, PV_NEW) = {info_new} ...")

            log.debug('Spliting df_master designators into rows...')
            df_master = split_into_rows(df_master, column='DESIGNATOR')
            df_master['DESIGNATOR'] = df_master['DESIGNATOR'].str.strip().str.upper()
            log.debug(f"\n{df_master.head(5).to_string(index=False)}")

            log.debug('Spliting df_new designators into rows...')
            df_new = split_into_rows(df_new, column='DESIGNATOR')
            df_new['DESIGNATOR'] = df_new['DESIGNATOR'].str.strip().str.upper()
            log.debug(f"\n{df_new.head(5).to_string(index=False)}")

            log.debug('Creating df_master_vs_new...')
            merge_dict = {'both': 'SAME', 'left_only': 'REMOVE', 'right_only': 'ADD'}
            df_master_vs_new = df_master.merge(df_new, how='outer', left_on=['DESIGNATOR'], right_on=['DESIGNATOR'], suffixes=['_MASTER', '_NEW'], indicator=True)
            df_master_vs_new['RESULT'] = df_master_vs_new['_merge'].map(merge_dict)
            df_master_vs_new['RESULT'] = np.where((df_master_vs_new['RESULT'] == 'SAME') & (df_master_vs_new['COMPONENT_MASTER'] != df_master_vs_new['COMPONENT_NEW']), 'SUB', df_master_vs_new['RESULT'])
            df_master_vs_new = df_master_vs_new.drop(['QUANTITY_MASTER', 'QUANTITY_NEW' , '_merge'], axis=1)
            df_master_vs_new['QUANTITY'] = 1
            log.debug(f"\n{df_master_vs_new.head(5).to_string(index=False)}")

            log.info(f"Writing df_master_vs_new with {str(len(df_master_vs_new))} rows into SCRIPT_OUTPUT.xlsx ...")
            output_folder = f"{path_main}\\recipe-bom-new\\{df_input.loc[i, 'BOM_NEW']}_{df_input.loc[i, 'MCTO_NEW']}_{df_input.loc[i, 'PV_NEW']}"
            if not os.path.exists(output_folder):
                log.info(f"Making folder = {output_folder} ...")
                os.makedirs(output_folder)
            df_master_vs_new.to_excel(f"{output_folder}\\SCRIPT_OUTPUT.xlsx", sheet_name='DETAIL', index=False)

            log.debug('Creating df_difference...')
            df_difference = df_master_vs_new.loc[(df_master_vs_new['RESULT'] != 'SAME')]
            df_difference = df_difference[['RESULT', 'COMPONENT_MASTER', 'COMPONENT_NEW', 'DESIGNATOR']]
            df_difference.drop_duplicates(subset=['DESIGNATOR'], keep='last', inplace=True)
            df_difference['COMPONENT_MASTER'] = df_difference['COMPONENT_MASTER'].fillna('NO PLACE')
            df_difference['COMPONENT_NEW'] = df_difference['COMPONENT_NEW'].fillna('NO PLACE')
            df_difference = df_difference.groupby(by=['RESULT', 'COMPONENT_MASTER', 'COMPONENT_NEW']).aggregate({'DESIGNATOR': lambda x: ','.join(sorted(x))}).reset_index()
            df_difference = df_difference.rename(columns={'COMPONENT_MASTER': 'PART NUMBER (WAS)', 'COMPONENT_NEW': 'PART NUMBER (IS)'})
            log.debug(f"\n{df_difference.head(5).to_string(index=False)}")

            if len(df_difference) < 1:
                df_difference = pd.DataFrame({'RESULT': ['SUB'], 'PART NUMBER (WAS)': ['DUMMY'], 'PART NUMBER (IS)': ['DUMMY'], 'DESIGNATOR': ['ALL']})
                log.info(f"No difference between master BOM and new BOM, added 1 dummy row")
            else:
                log.info(f"Found {str(len(df_difference))} differences between master BOM and new BOM")

            log.info(f"Writing df_difference with {str(len(df_difference))} rows into SCRIPT_OUTPUT.xlsx ...")
            with pd.ExcelWriter(f"{output_folder}\\SCRIPT_OUTPUT.xlsx", mode='a', if_sheet_exists='replace') as f:
                df_difference.to_excel(f, sheet_name="DIFFERENCE", index=False)

            log.debug('Splitting part number and designator...')
            partIsList =  list(df_difference['PART NUMBER (IS)'])
            partWasList =  list(df_difference['PART NUMBER (WAS)'])
            designatorsList =  list(df_difference['DESIGNATOR'])

            if len(partIsList) != len(partWasList) or len(partIsList) != len(designatorsList) or len(partWasList) != len(designatorsList):
                raise AssertionError(f"Number of partIs, partWas and designator does not tally, force skipping row #{i+1} (BOM_NEW, MCTO_NEW, PV_NEW) = {info_new}  ...")

            if 'NO PLACE' in (x.strip().upper() for x in partWasList):
                raise AssertionError(f"NO PLACE is found in partWasList, unable to handle part addition, force skipping force skipping row #{i+1} (BOM_NEW, MCTO_NEW, PV_NEW) = {info_new} ...")

            log.info(f"Total of {len(designatorsList)} line item(s) to be processed for row #{i+1} (BOM_NEW, MCTO_NEW, PV_NEW) = {info_new}")

            # Find for selected_program
            selected_program = [df_input.loc[i, 'PNP_PROGRAM_SIDE1_MASTER'], df_input.loc[i, 'PNP_PROGRAM_SIDE2_MASTER']]

            # log.debug('Hardcoding selected files...')
            # selected_program = ['3440CB-PD0-M5-IT', '3440CB-SD0-M5-IT']

            log.info(f"Selected_program = {selected_program}")

            log.debug("Scanning file_program...")
            file_program = {}

            # Recursively call scandir inclusive of subfolders for filename matching
            def scan_dir_file(path):
                for f in os.scandir(path):
                    if f.is_file() and (f.name[-3:].lower() == '.pp' or f.name[-4:].lower() == '.pp7') and any (matcher in f.name for matcher in selected_program):
                        yield f.path
                    elif f.is_dir():
                        yield from scan_dir_file(f.path)
            file_program = {f for f in scan_dir_file(path_recipe_bom_master)}
            file_program = sorted(file_program)

            log.info(f"Matched file_program = {file_program}")

            # Raise warning if no program file is found
            if len(file_program) < 1:
                raise AssertionError (f"There is no selected program file found, force skipping row #{i+1} (BOM_NEW, MCTO_NEW, PV_NEW) = {info_new} ...")


            # Loop through matched file_program
            for file in file_program:

                log.info(f"Processing: {file}...")

                filename = file.rsplit('\\', 1)[-1]
                log.debug(f"filename = {filename}")

                filename_without_ext = filename.rsplit('.', 1)[0]
                log.debug(f"filename_without_ext = {filename_without_ext}")

                file_ext = filename.rsplit('.', 1)[-1]
                log.debug(f"file_ext = {file_ext}")

                xmldata = file
                prstree = ETree.parse(xmldata)
                root = prstree.getroot()
            
                # Loop through line items to be processed
                for j in range(len(designatorsList)):
                    partIs, partWas, designators = partIsList[j].strip().upper(), partWasList[j].strip().upper(), designatorsList[j].strip().upper()
                    log.info(f"Processing partIs = {partIs}, partWas = {partWas}, designators = {designators} in {file} ...")
                    
                    # Expand and split designators
                    log.info(f"Expanding and spliting {designators} ...")
                    designators = ExpandSeries(designators)
                    if '-' in designators:
                        raise AssertionError(f"Designators {designators} not expanded and contains '-', force skipping row #{i+1} (BOM_NEW, MCTO_NEW, PV_NEW) = {info_new} ...")

                    designatorList = designators.split(',')
                    log.info(f"designatorList = {designatorList}")

                    # Handle .pp7 file
                    if file_ext.lower() == 'pp7':
                        pp_url = '{http://api.assembleon.com/pp7/v1}'
                        log.debug(f"Setting pp_url to {pp_url}...")

                        feeder_whitelist, check_designator, all_designator, componentToRemove, feederToRemove, actionToRemove, robotHeadToRemove = set(), set(), set(), set(), set(), set(), set()
                        for BoardInfo in root.iter(f"{pp_url}Board"):

                            # Modify program names for the 1st time processing the file only
                            if j == 0:
                                if 'SD' in BoardInfo.attrib['id'].strip().upper():
                                    sPROGRAM_NAME_MASTER = df_input.loc[i, 'PNP_PROGRAM_SIDE2_MASTER']
                                    sPROGRAM_NAME_NEW = df_input.loc[i, 'PNP_PROGRAM_SIDE2_NEW']
                                else:
                                    sPROGRAM_NAME_MASTER = df_input.loc[i, 'PNP_PROGRAM_SIDE1_MASTER']
                                    sPROGRAM_NAME_NEW = df_input.loc[i, 'PNP_PROGRAM_SIDE1_NEW']
                                log.info(f"Modifying program name from {sPROGRAM_NAME_MASTER} to {sPROGRAM_NAME_NEW} ...")
                                BoardInfo.attrib['id'] = BoardInfo.attrib['id'].replace(sPROGRAM_NAME_MASTER, sPROGRAM_NAME_NEW)
                                sPROGRAM_NAME = filename_without_ext.replace(sPROGRAM_NAME_MASTER, sPROGRAM_NAME_NEW)
                                log.info(f"sPROGRAM_NAME = {sPROGRAM_NAME}")

                            for ComponentInfo in BoardInfo.iter(f"{pp_url}Component"):
                                sPartNumber = ComponentInfo.attrib.get('partNumber')
                                sREFDES = ComponentInfo.attrib.get('refDes')

                                if sPartNumber == partWas and 'ALL' in designatorList: 
                                    log.debug(f"Adding {sREFDES} into all_designator...")
                                    all_designator.add(sREFDES)

                                elif sPartNumber == partWas and 'ALL' not in designatorList:
                                    log.debug(f"Adding {sREFDES} into check_designator...")
                                    check_designator.add(sREFDES)

                                for designator in designatorList:

                                    # Handle part removal, store all component info of the given component PN & REFDES to be deleted
                                    if partIs == 'NO PLACE' and sPartNumber == partWas and (sREFDES == designator or designator == 'ALL'):
                                        log.debug(f"Storing {sREFDES} {ComponentInfo} to be deleted in componentToRemove...")
                                        componentToRemove.add(ComponentInfo)

                                    # Handle part sub, modify the component part number of the given REFDES
                                    elif sPartNumber == partWas and (sREFDES == designator or designator == 'ALL'):
                                        log.info(f"Modifying {sREFDES} componenet part number from {partWas} to {partIs} ...")
                                        ComponentInfo.attrib['partNumber'] = ComponentInfo.attrib['partNumber'].replace(partWas, partIs)

                            # Handle part removal, delete all component in componentToRemove
                            for item in componentToRemove:
                                log.info(f"Deleting {item} in componentToRemove...")
                                BoardInfo.remove(item)
                            componentToRemove = set()

                        if len(all_designator) > 0:
                            log.info(f"Replacing designatorList with all_designator = {all_designator} ...")
                            designatorList = all_designator

                        elif len(check_designator) > 0:
                            log.info('Checking if all designators are included...')
                            for designator in designatorList:
                                if designator != 'ALL':
                                    try:
                                        check_designator.remove(designator)
                                    except:
                                        pass
                            if len(check_designator) > 0:
                                log.info(f"Not all designators are included, non-impacted designator = {check_designator}")
                            else:
                                log.info('All designators are included.')

                        for SegmentInfo in root.iter(f"{pp_url}Segment"):
                            for ProcessingInfo in SegmentInfo.iter(f"{pp_url}Processing"):
                                for BoardLocationInfo in ProcessingInfo.iter(f"{pp_url}BoardLocation"):
                                    for ActionInfo in BoardLocationInfo.iter(f"{pp_url}Action"):
                                        for PickInfo in ActionInfo.iter(f"{pp_url}Pick"):
                                            sREFDES = PickInfo.attrib.get('refDes')
                                            sRobotNumber = PickInfo.attrib.get('robotNumber')
                                            sHeadNumber = PickInfo.attrib.get('headNumber')

                                            for designator in designatorList:
                                                if partIs == 'NO PLACE' and sREFDES == designator:
                                                    # Handle part removal, store action info to be deleted in actionToRemove
                                                    log.debug(f"Storing {sREFDES} action with {PickInfo} in actionToRemove...")
                                                    actionToRemove.add(ActionInfo)

                                                    # Handle part removal, store robot and head to be deleted in robotHeadToRemove
                                                    robotHeadToRemove.add((sRobotNumber, sHeadNumber))
                                                    log.debug(f"Set of (RobotNumber, HeadNumber) to be removed = {robotHeadToRemove}.")

                                            # Store feeder for non-impacted designator into feeder_whitelist
                                            for non_impacted_designator in check_designator:
                                                if sREFDES == non_impacted_designator:
                                                    log.debug(f"Storing {sREFDES} Section {sSectionNumber}, Feeder {sFeederNumber}, Lane {sLaneNumber} into feeder_whitelist...")
                                                    feeder_whitelist.add((sSectionNumber, sFeederNumber, sLaneNumber))

                                    if len(robotHeadToRemove) > 0:
                                        for ActionInfo in BoardLocationInfo.iter(f"{pp_url}Action"):
                                            for AlignInfo in ActionInfo.iter(f"{pp_url}Align"):
                                                for robotHead in robotHeadToRemove:
                                                    if AlignInfo.attrib.get('robotNumber') == robotHead[0] and AlignInfo.attrib.get('headNumber') == robotHead[1]:
                                                        # Handle part removal, store action info to be deleted in actionToRemove
                                                        log.debug(f"Storing action with {AlignInfo} in actionToRemove...")
                                                        actionToRemove.add(ActionInfo)

                                            for PlaceInfo in ActionInfo.iter(f"{pp_url}Place"):
                                                for robotHead in robotHeadToRemove:
                                                    if PlaceInfo.attrib.get('robotNumber') == robotHead[0] and PlaceInfo.attrib.get('headNumber') == robotHead[1]:
                                                        # Handle part removal, store action info to be deleted in actionToRemove
                                                        log.debug(f"Storing action with {PlaceInfo} in actionToRemove...")
                                                        actionToRemove.add(ActionInfo)

                                    # Handle part removal, delete items in actionToRemove
                                    for item in actionToRemove:
                                        log.info(f"Deleting {item} in actionToRemove...")
                                        BoardLocationInfo.remove(item)
                                    actionToRemove = set()

                            for SetupInfo in SegmentInfo.iter(f"{pp_url}Setup"):
                                for FeedSectionInfo in SetupInfo.iter(f"{pp_url}FeedSection"):
                                    sSectionNumber = FeedSectionInfo.attrib.get('number')
                                    for FeederInfo in FeedSectionInfo.iter(f"{pp_url}Feeder"):
                                        sFeederNumber = FeederInfo.attrib.get('slotNumber')
                                        for LaneInfo in FeederInfo.iter(f"{pp_url}FeederLane"):
                                            sLaneNumber = LaneInfo.attrib.get('number')
                                            sPartNumber = LaneInfo.attrib.get('partNumber')

                                            # Handle part removal, store all feeder info in feederToRemove
                                            if partIs == 'NO PLACE' and sPartNumber == partWas:
                                                if len(check_designator) <= 0:
                                                    log.info('All designators included, good to delete the feeder.')
                                                    log.debug(f"Storing {sPartNumber} {FeederInfo} to be deleted in feederToRemove...")
                                                    feederToRemove.add(FeederInfo)
                                                else:
                                                    log.info('Not all designators are included, checking if the feeder is used on any non-impacted designator before deleting...')
                                                    # Skip if the feeder is used on any non-impacted designator
                                                    if (sSectionNumber, sFeederNumber, sLaneNumber) in feeder_whitelist:
                                                        log.debug(f"Skipping {sPartNumber}, Section {sSectionNumber}, Feeder {sFeederNumber}, Lane {sLaneNumber} to be deleted.")
                                                    else:
                                                        log.debug(f"Feeder only used in impacted_designator, storing {sPartNumber}, Section {sSectionNumber}, Feeder {sFeederNumber}, Lane {sLaneNumber} {FeederInfo} to be deleted in feederToRemove...")
                                                        feederToRemove.add(FeederInfo)

                                            # Handle part sub, modify the feeder lane part number
                                            elif sPartNumber == partWas:
                                                if len(check_designator) <= 0:
                                                    log.info('All designators included, good to modify the feeder.')
                                                    log.info(f"Modifying {FeederInfo} from {partWas} to {partIs} ...")
                                                    LaneInfo.attrib['partNumber'] = LaneInfo.attrib['partNumber'].replace(partWas, partIs)
                                                else:
                                                    log.info('Not all designators are included, checking if the feeder is used on any non-impacted designator before modifying...')
                                                    # Skip if the feeder is used on any non-impacted designator
                                                    if (sSectionNumber, sFeederNumber, sLaneNumber) in feeder_whitelist:
                                                        raise AssertionError(f"Same feeder is sharing for impacted and non-impacted designators, unable to modify feeder, force skipping row #{i+1} (BOM_NEW, MCTO_NEW, PV_NEW) = {info_new} ...")
                                                    else:
                                                        log.info(f"Modifying {FeederInfo} from {partWas} to {partIs} ...")
                                                        LaneInfo.attrib['partNumber'] = LaneInfo.attrib['partNumber'].replace(partWas, partIs)

                                    # Handle part removal, delete all feeder in feederToRemove
                                    for item in feederToRemove:
                                        log.info(f"Deleting {item} in feederToRemove...")
                                        FeedSectionInfo.remove(item)
                                    feederToRemove = set()


                    # Handle .pp file
                    else:
                        pp_url = '{http://api.assembleon.com/pp/v2}'
                        log.debug(f"Setting pp_url to {pp_url}...")

                        feeder_whitelist, check_designator, all_designator, componentToRemove, feederToRemove, actionToRemove = set(), set(), set(), set(), set(), set()
                        for BoardInfo in root.iter(f"{pp_url}Board"):

                            # Modify program names for the 1st time processing the file only
                            if j == 0:
                                if 'SD' in BoardInfo.attrib['id'].strip().upper():
                                    sPROGRAM_NAME_MASTER = df_input.loc[i, 'PNP_PROGRAM_SIDE2_MASTER']
                                    sPROGRAM_NAME_NEW = df_input.loc[i, 'PNP_PROGRAM_SIDE2_NEW']
                                else:
                                    sPROGRAM_NAME_MASTER = df_input.loc[i, 'PNP_PROGRAM_SIDE1_MASTER']
                                    sPROGRAM_NAME_NEW = df_input.loc[i, 'PNP_PROGRAM_SIDE1_NEW']
                                log.info(f"Modifying program name from {sPROGRAM_NAME_MASTER} to {sPROGRAM_NAME_NEW} ...")
                                BoardInfo.attrib['id'] = BoardInfo.attrib['id'].replace(sPROGRAM_NAME_MASTER, sPROGRAM_NAME_NEW)
                                sPROGRAM_NAME = filename_without_ext.replace(sPROGRAM_NAME_MASTER, sPROGRAM_NAME_NEW)
                                log.info(f"sPROGRAM_NAME = {sPROGRAM_NAME}")

                            for ComponentInfo in BoardInfo.iter(f"{pp_url}Component"):
                                sPartNumber = ComponentInfo.attrib.get('partNumber')
                                sREFDES = ComponentInfo.attrib.get('refDes')
                                
                                if sPartNumber == partWas and 'ALL' in designatorList: 
                                    log.debug(f"Adding {sREFDES} into all_designator...")
                                    all_designator.add(sREFDES)

                                elif sPartNumber == partWas and 'ALL' not in designatorList:
                                    log.debug(f"Adding {sREFDES} into check_designator...")
                                    check_designator.add(sREFDES)
                                
                                for designator in designatorList:

                                    # Handle part removal, store all component info of the given component PN & REFDES to be deleted
                                    if partIs == 'NO PLACE' and sPartNumber == partWas and (sREFDES == designator or designator == 'ALL'):
                                        log.debug(f"Storing {sREFDES} {ComponentInfo} to be deleted in componentToRemove...")
                                        componentToRemove.add(ComponentInfo)

                                    # Handle part sub, modify the component part number of the given REFDES
                                    elif sPartNumber == partWas and (sREFDES == designator or designator == 'ALL'):
                                        log.info(f"Modifying {sREFDES} componenet part number from {partWas} to {partIs} ...")
                                        ComponentInfo.attrib['partNumber'] = ComponentInfo.attrib['partNumber'].replace(partWas, partIs)

                            # Handle part removal, delete all component in componentToRemove
                            for item in componentToRemove:
                                log.info(f"Deleting {item} in componentToRemove...")
                                BoardInfo.remove(item)
                            componentToRemove = set()

                        if len(all_designator) > 0:
                            log.info(f"Replacing designatorList with all_designator = {all_designator} ...")
                            designatorList = all_designator

                        elif len(check_designator) > 0:
                            log.info('Checking if all designators are included...')
                            for designator in designatorList:
                                if designator != 'ALL':
                                    try:
                                        check_designator.remove(designator)
                                    except:
                                        pass
                            if len(check_designator) > 0:
                                log.info(f"Not all designators are included, non-impacted designator = {check_designator}")
                            else:
                                log.info('All designators are included.')

                        # Each section has 4 robots, total 5 sections with 20 robots, each with 1 head
                        sHeadNumber = '1'
                        robots_per_section = 4
                        section_number = 1
                        for a, ActionInfo in enumerate(root.iter(f"{pp_url}Actions")):
                            sSectionNumber = str(int(section_number))
                            if (a+1) % robots_per_section == 0:
                                section_number += 1
                            for IndexInfo in ActionInfo.iter(f"{pp_url}Index"):
                                # Enumerate to find the group from pick to place (Pick, Align, ReadFiducial, Place)
                                IndexInfoList = list(IndexInfo)
                                for k, IndexItem in enumerate(IndexInfoList):

                                    # Start whenever pick tag is found
                                    if IndexItem.tag == f"{pp_url}Pick":
                                        sREFDES = IndexItem.attrib.get('refDes')
                                        sFeederNumber = IndexItem.attrib.get('feederNumber')
                                        sLaneNumber = IndexItem.attrib.get('laneNumber')

                                        for designator in designatorList:
                                            # Handle part removal, store pick info in actionToRemove
                                            if partIs == 'NO PLACE' and sREFDES == designator:
                                                log.debug(f"Storing {sREFDES} {IndexItem} to be deleted in actionToRemove...")
                                                actionToRemove.add(IndexItem)

                                                # Handle part removal, store all subsequent tag after pick to place in actionToRemove
                                                y = k+1
                                                while y<len(IndexInfoList):
                                                    log.debug(f"Storing {sREFDES} {IndexInfoList[y]} to be deleted in actionToRemove...")
                                                    actionToRemove.add(IndexInfoList[y])

                                                    # Break if place tag is found
                                                    if IndexInfoList[y].tag == f"{pp_url}Place":
                                                        break
                                                    
                                                    y+=1
                                        
                                        # Store feeder for non-impacted designator into feeder_whitelist
                                        for non_impacted_designator in check_designator:
                                            if sREFDES == non_impacted_designator:
                                                log.debug(f"Storing {sREFDES}, Section {sSectionNumber}, Feeder {sFeederNumber}, Lane {sLaneNumber} into feeder_whitelist...")
                                                feeder_whitelist.add((sSectionNumber, sFeederNumber, sLaneNumber))

                                # Handle part removal, delete items in actionToRemove
                                for item in actionToRemove:
                                    try:
                                        log.info(f"Deleting {item} in actionToRemove...")
                                        IndexInfo.remove(item)
                                    except:
                                        # Skip item with no speciied info in it
                                        pass
                                actionToRemove = set()

                        for SectionInfo in root.iter(f"{pp_url}Section"):
                            sSectionNumber = SectionInfo.attrib.get('number')
                            for TrolleyInfo in SectionInfo.iter(f"{pp_url}Trolley"):
                                for FeederInfo in TrolleyInfo.iter(f"{pp_url}Feeder"):
                                    sFeederNumber = FeederInfo.attrib.get('number')
                                    for LaneInfo in FeederInfo.iter(f"{pp_url}Lane"):
                                        sLaneNumber = LaneInfo.attrib.get('number')
                                        sPartNumber = LaneInfo.attrib.get('partNumber')

                                        # Handle part removal, store all feeder info in feederToRemove
                                        if partIs == 'NO PLACE' and sPartNumber == partWas:
                                            if len(check_designator) <= 0:
                                                log.info('All designators included, good to delete the feeder.')
                                                log.debug(f"Storing {sPartNumber} {FeederInfo} to be deleted in feederToRemove...")
                                                feederToRemove.add(FeederInfo)
                                            else:
                                                log.info('Not all designators are included, checking if the feeder is used on any non-impacted designator before deleting...')
                                                # Skip if the feeder is used on any non-impacted designator
                                                if (sSectionNumber, sFeederNumber, sLaneNumber) in feeder_whitelist:
                                                    log.debug(f"Skipping {sPartNumber}, Section {sSectionNumber}, Feeder {sFeederNumber}, Lane {sLaneNumber} to be deleted.")
                                                else:
                                                    log.debug(f"Feeder only used in impacted_designator, storing {sPartNumber}, Section {sSectionNumber}, Feeder {sFeederNumber}, Lane {sLaneNumber} {FeederInfo} to be deleted in feederToRemove...")
                                                    feederToRemove.add(FeederInfo)

                                        # Handle part sub, modify the feeder lane part number
                                        elif sPartNumber == partWas:
                                            if len(check_designator) <= 0:
                                                log.info('All designators included, good to modify the feeder.')
                                                log.info(f"Modifying {FeederInfo} from {partWas} to {partIs} ...")
                                                LaneInfo.attrib['partNumber'] = LaneInfo.attrib['partNumber'].replace(partWas, partIs)
                                            else:
                                                log.info('Not all designators are included, checking if the feeder is used on any non-impacted designator before modifying...')
                                                # Skip if the feeder is used on any non-impacted designator
                                                if (sSectionNumber, sFeederNumber, sLaneNumber) in feeder_whitelist:
                                                    raise AssertionError(f"Same feeder is sharing for impacted and non-impacted designators, unable to modify feeder, force skipping row #{i+1} (BOM_NEW, MCTO_NEW, PV_NEW) = {info_new} ...")
                                                else:
                                                    log.info(f"Modifying {FeederInfo} from {partWas} to {partIs} ...")
                                                    LaneInfo.attrib['partNumber'] = LaneInfo.attrib['partNumber'].replace(partWas, partIs)

                                # Handle part removal, delete all feeder in feederToRemove
                                for item in feederToRemove:
                                    log.info(f"Deleting {item} in feederToRemove...")
                                    TrolleyInfo.remove(item)
                                feederToRemove = set()


                # Write to a new file
                output_folder = f"{path_main}\\recipe-bom-new\\{df_input.loc[i, 'BOM_NEW']}_{df_input.loc[i, 'MCTO_NEW']}_{df_input.loc[i, 'PV_NEW']}"
                if not os.path.exists(output_folder):
                    log.info(f"Making folder = {output_folder} ...")
                    os.makedirs(output_folder)
                output_path = f"{output_folder}\\{sPROGRAM_NAME}.{file_ext}"
                log.info(f"Writing into {output_path}...")
                with open(f"{output_path}", 'wb') as f:
                    prstree.write(f)

        except AssertionError as e:
            log.warning(f"{str(e)}")
            output_folder = f"{path_main}\\recipe-bom-new\\{df_input.loc[i, 'BOM_NEW']}_{df_input.loc[i, 'MCTO_NEW']}_{df_input.loc[i, 'PV_NEW']}"
            if os.path.exists(output_folder):
                log.warning(f"Found output folder with warning, deleting {output_folder} ...")
                delete_file(output_folder)
            continue

    log.info('Successfully completed without any errors!!!')
    log.info('Closing application...')
    time.sleep(5)

    return


if __name__ == '__main__':
    try:
        log, path_main, path_590, path_MCTO, path_recipe_bom_master, path_recipe_bom_new, path_bom = init()
        main(log, path_main, path_590, path_MCTO, path_recipe_bom_master, path_recipe_bom_new, path_bom)

    except ConnectionAbortedError as e:
        log.error(f"{str(e)}")
        time.sleep(5)
        sys.exit(0)

    except Exception as e:
        log.critical(f"{str(e)}")
        log.exception(f"Unexpected Error, force exiting application...")
        time.sleep(5)