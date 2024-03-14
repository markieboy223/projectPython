from pathlib import Path
from loguru import logger

class Settings:
    # Serverinstellingen
    servername = r'LAPTOP-NDOACQTH\SQLEXPRESS'
    database = 'PR4'

    # Bestandspaden
    inventory_levels_csv = Path('../GO_SALES_INVENTORY_LEVELSData.csv')
    go_sales_sqlite = Path('../go_sales.sqlite')
    go_staff_sqlite = Path('../go_staff.sqlite')
    go_crm_sqlite = Path('../go_crm.sqlite')
    go_sales_product_forecast_csv = Path('../GO_SALES_PRODUCT_FORECASTData.csv')

# Logger instellen
logger.add("script.log", rotation="10 MB")  # Logbestand aanmaken

# Een instantie van de Settings-klasse maken
settings = Settings()
