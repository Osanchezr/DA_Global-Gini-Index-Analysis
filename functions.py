import pandas as pd

def load_data(paths):
    """
    Carga los datos desde las rutas especificadas.
    
    Args:
    paths (dict): Diccionario con los df y sus rutas de cada archivo.
    
    Returns:
    dict: Diccionario con df(nombre) y dataFrames cargados.
    """
    dfs = {key: pd.read_csv(path) for key, path in paths.items()}
    return dfs

def clean_columns(dfs, rename_columns_dict):

    """
    Limpia y renombra las columnas en los DataFrames proporcionados.
     -convierte a minuscula y rellena los espacios con "_" en las columnas de cada df.
     -renombra columnas con disccionario especificado

    Args:
    dfs (dict): Diccionario de DataFrames a limpiar.
    rename_columns_dict (dict): Diccionario con los cambios de nombre por DataFrame.
        Ejemplo: {"df3": {"expected_years_of_schooling": "expected_years_school"}}
    
    Returns:
    dict: DataFrames con columnas limpias.
    """
    for key, df in dfs.items():
        df.columns = df.columns.str.lower().str.replace(' ', '_')
        if key in rename_columns_dict:
            df.rename(columns=rename_columns_dict[key], inplace=True)
    
    return dfs

def merge_dfs(dfs, merge_keys, join_columns):
    """
    -Crea columna "codigo" en df1,df2.df3 basado en "merge_keys" para relacionar dfs 
    -Selecciona solo columnas a usar antes de la fusión.
    -Realiza el merge en los dfs

    Args:
    dfs (dict): Diccionario de DataFrames a unir.
    merge_keys (dict): Diccionario de listas con las columnas clave para crear la columna 'codigo'.
    join_columns (dict): Diccionario con las columnas a seleccionar de cada DataFrame.

    Returns:
    pd.DataFrame: DataFrame unido.
    """
    # Proceso para df1
    df_merged = dfs['df1']
    df_merged["codigo"] = df_merged[merge_keys['df1'][0]].astype(str) + "_" + df_merged[merge_keys['df1'][1]].astype(str)
    
    df_merged = df_merged[join_columns['df1']]
    
    # Proceso para dfs restantes
    for key in dfs:
        if key != 'df1':
            dfs[key]["codigo"] = dfs[key][merge_keys[key][0]].astype(str) + "_" + dfs[key][merge_keys[key][1]].astype(str)
            dfs[key] = dfs[key][join_columns[key]]
            
            if key != "df4":
                df_merged = pd.merge(df_merged, dfs[key], on='codigo', how='left')

            else:
                df_merged = pd.merge(df_merged, dfs[key], on='country_code', how='left')    

    return df_merged

def convertir_anio_a_fecha(df, columna_anio, formato='%Y'):
    """
    Convierte una columna de años enteros a fechas con el formato de datetime.

    Args:
    df (pd.DataFrame): El DataFrame que contiene la columna de años.
    columna_anio (str): El nombre de la columna que contiene los años.
    formato (str): El formato del año a convertir. Por defecto '%Y'.

    Returns:
    pd.DataFrame: El DataFrame con la columna de años convertida a datetime.
    """
    df[columna_anio] = pd.to_datetime(df[columna_anio], format=formato)
    return df

def limit_analitic(df, columna_anio, start_year=None, end_year=None):
    """
    Limita el df considerando los años de inicio y final para el análisis.

    Args:
    df (pd.DataFrame): El DataFrame que contiene la columna de años.
    columna_anio (str): El nombre de la columna que contiene los años.
    formato (str): El formato del año a convertir. Por defecto '%Y'.
    start_year (int): Año de inicio del rango. Si se proporciona, limita el DataFrame a partir de este año.
    end_year (int): Año final del rango. Si se proporciona, limita el DataFrame hasta este año.

    Returns:
    pd.DataFrame: El DataFrame con la columna de años convertida a datetime y limitado al rango especificado.
    """
    if start_year is not None and end_year is not None:
        df = df[(df[columna_anio].dt.year >= start_year) & (df[columna_anio].dt.year <= end_year)]
    
    return df

def clean_nulls(df, columns_to_fill, group_by_column, fill_method='mean', thresh=None):
    """
    Limpia los valores nulos en el DataFrame
    -LLena valores nulos en columnas especificadas segun media de grupos considerados.
    -Eliminacion de filas con mas de 3 valores nulos.
    -Eliminacion de valores nulos en columnas especificadas

    Args:
    df (pd.DataFrame): DataFrame a limpiar.
    columns_to_fill (list): Lista de columnas a llenar valores nulos.
    group_by_column (str): Columna para agrupar antes de llenar los nulos.
    fill_method (str): Método para llenar nulos ('mean' o 'median').
    thresh (int): Número mínimo de datos no nulos por fila para mantenerla.

    Returns:
    pd.DataFrame: DataFrame limpio.
    """
    for col in columns_to_fill:
        if fill_method == 'mean':
            df[col] = df.groupby(group_by_column)[col].transform(lambda x: x.fillna(x.mean()))
        elif fill_method == 'median':
            df[col] = df.groupby(group_by_column)[col].transform(lambda x: x.fillna(x.median()))
    
    num_columns = len(df.columns)
    thresh = thresh if thresh else num_columns - 3
    df = df.dropna(thresh=thresh)

    df = df.dropna(subset=columns_to_fill)

    return df