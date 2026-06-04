#!/usr/bin/env python3
# stream_chicago.py

import socket
import time
import csv
import sys

CSV_FILE = "/root/chicago_air.csv"
HOST     = "localhost"
PORT     = 9999
INTERVAL = 60 # secondes entre chaque batch de données

def parse_line(row):
    """Extrait neighborhood et pm25 depuis une ligne du CSV."""
    try:
        if len(row) < 48:
            return None
        sensor_name = row[3].strip().replace('"', '')
        pm25_val    = row[24].strip().replace('"', '').replace(',', '.')
        if not pm25_val or sensor_name.startswith('sensor_name'):
            return None
        float(pm25_val)
        neighborhood = sensor_name
        import re
        neighborhood = re.sub(r'\s+\d+$', '', neighborhood).strip()
        return f"{neighborhood},{pm25_val}"
    except Exception:
        return None

def main():
    print(f"Démarrage du serveur stream sur {HOST}:{PORT}")
    print(f"Lecture du dataset : {CSV_FILE}")

    server = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    server.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
    server.bind((HOST, PORT))
    server.listen(1)

    print(f"En attente de connexion Spark sur port {PORT}...")
    conn, addr = server.accept()
    print(f"Spark connecté depuis {addr}")

    batch_size = 10  # lignes par batch (toutes les 60 secondes)
    batch      = []
    total_sent = 0

    with open(CSV_FILE, 'r', encoding='utf-8') as f:
        reader = csv.reader(f)
        next(reader)

        for row in reader:
            line = parse_line(row)
            if line:
                batch.append(line)

            if len(batch) >= batch_size:
                try:
                    message = "\n".join(batch) + "\n"
                    conn.sendall(message.encode('utf-8'))
                    total_sent += len(batch)
                    print(f"[{time.strftime('%H:%M:%S')}] "
                          f"Batch envoyé : {len(batch)} lignes "
                          f"(total : {total_sent})")
                    batch = []
                    time.sleep(INTERVAL)
                except BrokenPipeError:
                    print("Spark déconnecté. Fin du stream.")
                    break

    # Envoyer le dernier batch
    if batch:
        try:
            conn.sendall(("\n".join(batch) + "\n").encode('utf-8'))
        except:
            pass

    print(f"\nStream terminé. {total_sent} lignes envoyées.")
    conn.close()
    server.close()

if __name__ == "__main__":
    main()
