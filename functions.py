import pandas as pd

def load_data(paths):
    """
    Carga los datos desde las rutas especificadas.
    
    Args:
    paths (dict): Diccionario con las rutas de los archivos.
    
    Returns:
    dict: Diccionario con DataFrames cargados.
    """
    dfs = {key: pd.read_csv(path) for key, path in paths.items()}
    return dfs

def clean_columns(df1, df3, df5, df6):
    """
    Limpia y renombra las columnas en los DataFrames proporcionados.
    
    Args:
    df1, df3, df5, df6 (pd.DataFrame): DataFrames a limpiar.
    
    Returns:
    tuple: DataFrames limpios.
    """ 
    df1.columns = df1.columns.str.lower().str.replace(' ', '_')
    df3.columns = df3.columns.str.lower().str.replace(' ', '_')
    df5.columns = df5.columns.str.lower().str.replace(' ', '_')
    df6.columns = df6.columns.str.lower().str.replace(' ', '_')


    df3.rename(columns={"expected_years_of_schooling": "expected_years_school"}, inplace=True)
    df5.rename(columns={"historical_and_more_recent_expenditure_estimates": "spend_public_education"}, inplace=True)  
    df6.rename(columns={"alpha-3_code": "country_code", "latitude_(average)": "latitude", "longitude_(average)": "longitude"}, inplace=True)
      
    return df1, df3, df5, df6

def merge_dfs(df1, df3, df5, df6):
    """
    Une los DataFrames en uno solo basado en la columna 'codigo' y 'country_code'.
    
    Args:
    df1, df3, df5, df6 (pd.DataFrame): DataFrames a unir.
    
    Returns:
    pd.DataFrame: DataFrame unido.
    """
    df1["codigo"] = df1['country_code'] + "_" + df1['reporting_year'].astype(str)
    df3["codigo"] = df3['code'] + "_" + df3['year'].astype(str)
    df5["codigo"] = df5['code'] + "_" + df5['year'].astype(str)

    df1 = df1[['region_name', 'region_code', 'country_name', 'country_code','reporting_year','gini','poverty_line', 'headcount','poverty_gap','reporting_pop', 'reporting_gdp',"codigo"]]
    df3 = df3[["codigo", "expected_years_school"]]
    df5 = df5[["codigo", "spend_public_education"]]
    df6 = df6[["country_code", "latitude", "longitude"]]

    df_merged = pd.merge(df1, df3, on='codigo', how='left')
    df_merged = pd.merge(df_merged, df5, on='codigo', how='left')
    df_merged = pd.merge(df_merged, df6, on='country_code', how='left')

    return df_merged

def limit_analitic(df):
    """
    Limita el DataFrame a los datos entre los años 2000 y 2019.
    
    Args:
    df (pd.DataFrame): DataFrame a limitar.
    
    Returns:
    pd.DataFrame: DataFrame limitado.
    """
    df = df[(df["reporting_year"] >= 2000) & (df["reporting_year"] <= 2019)]
    return df

def clean_nulls(df):
    """
    Limpia los valores nulos en el DataFrame, manteniendo solo filas con suficientes datos y llenando valores nulos.
    
    Args:
    df (pd.DataFrame): DataFrame a limpiar.
    
    Returns:
    pd.DataFrame: DataFrame limpio.
    """
    df['expected_years_school'] = df.groupby('country_name')['expected_years_school'].transform(lambda x: x.fillna(x.mean()))
    df['spend_public_education'] = df.groupby('country_name')['spend_public_education'].transform(lambda x: x.fillna(x.mean()))
    num_columns = len(df.columns)
    df = df.dropna(thresh=num_columns - 3)  
    df = df.dropna(subset=["expected_years_school", "spend_public_education"])
    return df

def convertir_anio_a_fecha(df, columna_anio):
    """
    Convierte una columna de años enteros a fechas con el formato de datetime.
    
    Parámetros:
    df (pd.DataFrame): El DataFrame que contiene la columna de años.
    columna_anio (str): El nombre de la columna que contiene los años.
    
    Retorna:
    pd.DataFrame: El DataFrame con la columna de años convertida a datetime.
    """
    # Convertir la columna de años a datetime (1 de enero por defecto)
    df[columna_anio] = pd.to_datetime(df[columna_anio], format='%Y')
    
    return df