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
    df1 = df1[['region_name', 'region_code', 'country_name', 'country_code','reporting_year','gini','poverty_line', 'headcount','poverty_gap','reporting_pop', 'reporting_gdp']]
    df1["codigo"] = df1['country_code'] + "_" + df1['reporting_year'].astype(str)
    
    df3["codigo"] = df3['Code'] + "_" + df3['Year'].astype(str)
    df3 = df3[["codigo", "Expected years of schooling"]]
    df3.rename(columns={"Expected years of schooling": "expected_years_school"}, inplace=True)
    
    df5["codigo"] = df5['Code'] + "_" + df5['Year'].astype(str)
    df5 = df5[["codigo", "Historical and more recent expenditure estimates"]]
    df5.rename(columns={"Historical and more recent expenditure estimates": "spend_public_education"}, inplace=True)
    
    df6.rename(columns={"Alpha-3 code": "country_code", "Latitude (average)": "Latitude", "Longitude (average)": "Longitude"}, inplace=True)
    df6 = df6[["country_code", "Latitude", "Longitude"]]
    
    return df1, df3, df5, df6

def merge_df(df1, df3, df5, df6):
    """
    Une los DataFrames en uno solo basado en la columna 'codigo' y 'country_code'.
    
    Args:
    df1, df3, df5, df6 (pd.DataFrame): DataFrames a unir.
    
    Returns:
    pd.DataFrame: DataFrame unido.
    """
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

def valuenull(df):
    """
    Limpia los valores nulos en el DataFrame, manteniendo solo filas con suficientes datos y llenando valores nulos.
    
    Args:
    df (pd.DataFrame): DataFrame a limpiar.
    
    Returns:
    pd.DataFrame: DataFrame limpio.
    """
    num_columns = len(df.columns)
    df = df.dropna(thresh=num_columns - 3)
    
    df['expected_years_school'] = df.groupby('country_name')['expected_years_school'].transform(lambda x: x.fillna(x.mean()))
    df['spend_public_education'] = df.groupby('country_name')['spend_public_education'].transform(lambda x: x.fillna(x.mean()))
    
    df = df.dropna(subset=["expected_years_school", "spend_public_education"])
    
    return df

# Ejemplo de uso
if __name__ == "__main__":
    # Rutas de los archivos
    paths = {
        "df1": "C:/Users/osanc/Desktop/IronHack/Sem_3/Proyecto/pip.csv",
        "df3": "C:/Users/osanc/Desktop/IronHack/Sem_3/Proyecto/expected-years-of-schooling-vs-share-in-extreme-poverty.csv",
        "df5": "C:/Users/osanc/Desktop/IronHack/Sem_3/Proyecto/total-government-expenditure-on-education-gdp.csv",
        "df6": "C:/Users/osanc/Desktop/IronHack/Sem_3/Proyecto/country-coord.csv"
    }
    
    # Cargar datos
    dfs = load_data(paths)
    
    # Limpiar columnas
    df1, df3, df5, df6 = clean_columns(dfs["df1"], dfs["df3"], dfs["df5"], dfs["df6"])
    
    # Unir DataFrames
    df_merged = merge_df(df1, df3, df5, df6)
    
    # Limitar datos
    df_merged = limit_analitic(df_merged)
    
    # Limpiar valores nulos
    df_merged = valuenull(df_merged)
    
    # Mostrar el DataFrame final
    print(df_merged.head())
