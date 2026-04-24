import pandas as pd
import logging
from pathlib import Path

# Configuración básica de logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

def drop_duplicate_player(df):
    """Elimina jugadores duplicados con sufijos '3TM' o '2TM'."""
    duplicated_players = df[df['Team'].isin(['3TM', '2TM'])]
    drop_idx = []
    for name in duplicated_players['Player'].unique():
        player_rows = df[df['Player'] == name]
        player_to_keep = player_rows[~player_rows['Team'].isin(['3TM', '2TM'])].head(1)
        drop_idx.extend(player_rows.index.difference(player_to_keep.index))
    df.drop(drop_idx, axis=0, inplace=True)
    return df

def clean_data(url):
    """Limpia y transforma los datos de la NBA."""
    logger.info("Extrayendo datos de la URL...")
    df = pd.read_html(url)[0].copy()
    
    # Eliminar columnas no necesarias
    cols_drop = [col for col in df.columns if (col.__contains__('Unnamed:')) | (col == 'Rk')]
    df.drop(cols_drop, axis=1, inplace=True)
    
    # Convertir columnas numéricas
    cols_cat = ['Player', 'Pos', 'Team', 'Awards']
    for col in df.columns:
        if col not in cols_cat:
            df[col] = pd.to_numeric(df[col], errors='ignore')
    
    # Limpieza adicional
    df = df.fillna(0)
    df['Pos'] = df['Pos'].apply(lambda x: str(x).split('-')[0] if pd.notna(x) else 'Unknown')
    df = drop_duplicate_player(df)
    
    logger.info("Datos transformados correctamente.")
    return df

def save_clean_data(df, output_path):
    """Guarda los datos limpios en un archivo CSV."""
    Path(output_path).parent.mkdir(exist_ok=True)  # Crea la carpeta si no existe
    df.to_csv(output_path, index=False)
    logger.info(f"Datos guardados en {output_path}")

def run_etl():
    url = 'https://www.basketball-reference.com/leagues/NBA_2021_totals.html'
    output_file = "../data/processed/nba_player_stats_2021_clean.csv"
    
    try:
        # EXTRACT & TRANSFORM
        cleaned_df = clean_data(url)
        
        # LOAD (guardar en CSV)
        save_clean_data(cleaned_df, output_file)
        
    except Exception as e:
        logger.error(f"Error en el pipeline ETL: {e}", exc_info=True)
        raise

if __name__ == "__main__":
    run_etl()