#!/usr/bin/env python3
"""
PRODUCTEUR OPEN-METEO → HBASE (VERSION RAPIDE)
API SIMPLE • INSERTIONS RAPIDES • IDEAL POUR DATASET ML
"""

import sys
import time
import json
from datetime import datetime

print("="*60, flush=True)
print("🌦 PRODUCTEUR: Open-Meteo → HBase (FAST MODE)", flush=True)
print("="*60, flush=True)

# --------------------------
# CONFIGURATION
# --------------------------
API_URL = (
    "https://api.open-meteo.com/v1/forecast?"
    "latitude=51.50&longitude=-0.12&current_weather=true"
)

TARGET_RECORDS = 20000
HBASE_HOST = "hbase-standalone-bd"
HBASE_PORT = 9090

print(f"🎯 Objectif: {TARGET_RECORDS} enregistrements", flush=True)

# --------------------------
# IMPORT MODULES
# --------------------------
try:
    import requests
    print("✅ requests OK")
except:
    print("❌ Installer requests : pip install requests")
    sys.exit(1)

try:
    import happybase
    print("✅ happybase OK")
    USE_HBASE = True
except:
    print("⚠️ happybase NON disponible – mode fichier")
    USE_HBASE = False


# --------------------------
# CONNEXION HBASE
# --------------------------
def connect_hbase():
    if not USE_HBASE:
        return None
    
    try:
        print("🔌 Connexion HBase…")
        conn = happybase.Connection(HBASE_HOST, port=HBASE_PORT)
        print("✅ Connecté à HBase")
        return conn
    except Exception as e:
        print(f"❌ Erreur HBase : {e}")
        return None


# --------------------------
# CRÉATION TABLE
# --------------------------
def create_table(conn):
    if not conn:
        return
    
    tables = conn.tables()

    if b"weather_history" not in tables:
        print("📦 Création table weather_history…")
        conn.create_table("weather_history", {
            "data": dict(),
            "raw": dict()
        })
        print("✅ Table créée")
    else:
        print("✅ Table weather_history existe déjà")


# --------------------------
# APPEL API
# --------------------------
def fetch_weather():
    try:
        r = requests.get(API_URL, timeout=10)
        if r.status_code == 200:
            json_data = r.json()
            return json_data.get("current_weather", {})
        else:
            print(f"❌ HTTP {r.status_code}")
            return None
    except Exception as e:
        print(f"❌ API Error: {e}")
        return None


# --------------------------
# ENREGISTREMENT HBASE
# --------------------------
def save_to_hbase(conn, w):
    if not conn or not w:
        return 0
    
    table = conn.table("weather_history")
    rowkey = f"w_{int(time.time()*1000)}".encode()  # millisecondes → unique

    row = {
        b"data:temperature": str(w.get("temperature")).encode(),
        b"data:windspeed": str(w.get("windspeed")).encode(),
        b"data:winddirection": str(w.get("winddirection")).encode(),
        b"data:weathercode": str(w.get("weathercode")).encode(),
        b"data:time": str(w.get("time")).encode(),
        b"raw:json": json.dumps(w).encode()
    }

    table.put(rowkey, row)
    return 1


# --------------------------
# MODE BACKUP
# --------------------------
def save_to_file(w):
    filename = f"/data/weather_{int(time.time()*1000)}.json"
    with open(filename, "w") as f:
        json.dump(w, f, indent=2)
    return filename


# --------------------------
# MAIN LOOP (FAST MODE)
# --------------------------
def main():
    print("⏳ Lancement producteur…", flush=True)
    time.sleep(3)

    conn = connect_hbase()
    if conn:
        try:
            create_table(conn)
        except Exception as e:
            print(f"⚠️ Erreur création table: {e}", flush=True)

    total = 0

    while total < TARGET_RECORDS:
        try:
            w = fetch_weather()

            if w:
                if conn:
                    try:
                        saved = save_to_hbase(conn, w)
                    except Exception as e:
                        print(f"❌ Erreur Insert HBase: {e}", flush=True)
                        saved = 0
                        # Try to reconnect
                        conn = connect_hbase()
                else:
                    save_to_file(w)
                    saved = 1

                total += saved
                if total % 10 == 0:
                     print(f"📊 Total: {total}/{TARGET_RECORDS}", flush=True)

            # ****** MODE RAPIDE ******
            time.sleep(0.05)   # ← 40× PLUS RAPIDE
        except Exception as e:
             print(f"❌ Crash Loop: {e}", flush=True)
             time.sleep(5)

    print("\n🎉 OBJECTIF ATTEINT !!", flush=True)
    print(f"Total: {total} enregistrements")


if __name__ == "__main__":
    main()
