import pandas as pd
from pi_modules.utils import PIWebAPI



conn  = PIWebAPI(base_url="https://egpna-pi-web.enelint.global", auth="Kerberos")
data = conn.compressed_data(dataserver="AndvrPIDatArcha.enelint.global", taglist=["S BLUJAY INVERTERS INV-B3 VAL P KW", "S BLUJAY INVERTERS INV-B6 VAL P KW"], starttime="y-1d", endtime="t")
print(data)