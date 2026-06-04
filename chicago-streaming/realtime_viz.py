#!/usr/bin/env python3
# realtime_viz.py
# Visualisation temps réel des résultats Spark Streaming
# Lancer dans un 3ème terminal séparé

import subprocess
import time
import matplotlib
matplotlib.use('Agg')
import matplotlib.pyplot as plt
import matplotlib.gridspec as gridspec
from collections import defaultdict
import os

OUTPUT_DIR    = "/chicago/streaming/output"
VIZ_OUTPUT    = "/root/realtime_chicago.png"
REFRESH_SECS  = 60

def get_latest_hdfs_results():
    """Récupère les derniers résultats depuis HDFS."""
    try:
        # Lister les batches disponibles
        res = subprocess.run(
            ['hdfs', 'dfs', '-ls', OUTPUT_DIR],
            capture_output=True, text=True
        )
        if res.returncode != 0:
            return {}

        # Prendre les 3 derniers batches
        lines = [l for l in res.stdout.strip().split('\n')
                 if 'batch_' in l]
        if not lines:
            return {}

        lines.sort()
        recent = lines[-3:]  # 3 derniers = fenêtre glissante 9s

        data = defaultdict(list)
        for l in recent:
            path = l.split()[-1]
            cat = subprocess.run(
                ['hdfs', 'dfs', '-cat', path + '/part-*'],
                capture_output=True, text=True
            )
            for row in cat.stdout.strip().split('\n'):
                row = row.strip().strip('(').strip(')')
                if ',' not in row:
                    continue
                try:
                    parts = row.split(',')
                    hood  = parts[0].strip()
                    pm25  = float(parts[1].strip())
                    data[hood].append(pm25)
                except:
                    continue
        return data
    except Exception as e:
        print(f"Erreur HDFS: {e}")
        return {}

def make_plot(data, iteration):
    """Génère le graphique temps réel."""
    if not data:
        print("Pas de données disponibles encore...")
        return

    # Calculer les moyennes
    avgs = {k: sum(v)/len(v) for k, v in data.items()}
    avgs = dict(sorted(avgs.items(), key=lambda x: -x[1]))

    fig = plt.figure(figsize=(16, 10))
    fig.patch.set_facecolor('#1a1a2e')
    fig.suptitle(
        f'Chicago Air Quality — Stream temps réel '
        f'(batch #{iteration} — {time.strftime("%H:%M:%S")})',
        fontsize=14, fontweight='bold', color='white', y=0.98
    )
    gs = gridspec.GridSpec(2, 2, figure=fig, hspace=0.45, wspace=0.35)

    # Couleurs dark theme
    ax_bg    = '#16213e'
    bar_red  = '#e74c3c'
    bar_blue = '#3498db'

    # --- Graphique 1 : Bar chart quartiers ---
    ax1 = fig.add_subplot(gs[0, 0])
    ax1.set_facecolor(ax_bg)
    top = dict(list(avgs.items())[:10])
    bars = ax1.barh(list(top.keys()), list(top.values()),
                    color=bar_red, alpha=0.85, edgecolor='white', linewidth=0.5)
    ax1.axvline(12, color='#f39c12', linestyle='--',
                linewidth=1.5, label='Seuil OMS 12 µg/m³')
    ax1.set_xlabel('PM2.5 moyen (µg/m³)', color='white')
    ax1.set_title('Top quartiers pollués (temps réel)',
                  color='white', fontsize=10)
    ax1.tick_params(colors='white', labelsize=8)
    ax1.spines[:].set_color('#444')
    ax1.legend(fontsize=7, facecolor='#1a1a2e', labelcolor='white')
    # Ajouter les valeurs sur les barres
    for bar, val in zip(bars, top.values()):
        ax1.text(bar.get_width() + 0.1, bar.get_y() + bar.get_height()/2,
                 f'{val:.1f}', va='center', color='white', fontsize=7)

    # --- Graphique 2 : Gauge PM2.5 moyen global ---
    ax2 = fig.add_subplot(gs[0, 1])
    ax2.set_facecolor(ax_bg)
    global_avg = sum(avgs.values()) / len(avgs) if avgs else 0
    nb_quartiers = len(avgs)

    # Cercle de jauge
    theta = [i * 0.01 for i in range(315)]
    ax2.set_xlim(-1.5, 1.5)
    ax2.set_ylim(-1.5, 1.5)
    ax2.axis('off')

    # Couleur selon niveau
    if global_avg <= 12:
        color_gauge = '#2ecc71'
        niveau = 'BON'
    elif global_avg <= 35:
        color_gauge = '#f39c12'
        niveau = 'MODERE'
    else:
        color_gauge = '#e74c3c'
        niveau = 'MAUVAIS'

    circle_bg = plt.Circle((0, 0), 1.0, color='#2c3e50',
                            fill=True, linewidth=2)
    circle_fg = plt.Circle((0, 0), 1.0, color=color_gauge,
                            fill=False, linewidth=8)
    ax2.add_patch(circle_bg)
    ax2.add_patch(circle_fg)
    ax2.text(0, 0.15, f'{global_avg:.1f}', ha='center', va='center',
             fontsize=28, fontweight='bold', color='white')
    ax2.text(0, -0.2, 'µg/m³ PM2.5', ha='center', va='center',
             fontsize=10, color='#aaa')
    ax2.text(0, -0.5, niveau, ha='center', va='center',
             fontsize=14, fontweight='bold', color=color_gauge)
    ax2.text(0, -0.8, f'{nb_quartiers} quartiers actifs',
             ha='center', va='center', fontsize=9, color='#aaa')
    ax2.set_title('Indice PM2.5 global', color='white', fontsize=10)

    # --- Graphique 3 : Comparaison top 5 vs seuil OMS ---
    ax3 = fig.add_subplot(gs[1, 0])
    ax3.set_facecolor(ax_bg)
    top5 = dict(list(avgs.items())[:5])
    x = range(len(top5))
    bars3 = ax3.bar(x, list(top5.values()),
                    color=[bar_red if v > 12 else '#2ecc71'
                           for v in top5.values()],
                    alpha=0.85, edgecolor='white', linewidth=0.5)
    ax3.axhline(12, color='#f39c12', linestyle='--',
                linewidth=2, label='Seuil OMS')
    ax3.set_xticks(list(x))
    ax3.set_xticklabels(list(top5.keys()),
                        rotation=30, ha='right',
                        fontsize=7, color='white')
    ax3.set_ylabel('PM2.5 (µg/m³)', color='white')
    ax3.set_title('Top 5 vs Seuil OMS (rouge = dépassement)',
                  color='white', fontsize=9)
    ax3.tick_params(colors='white')
    ax3.spines[:].set_color('#444')
    ax3.legend(fontsize=8, facecolor='#1a1a2e', labelcolor='white')
    for bar, val in zip(bars3, top5.values()):
        ax3.text(bar.get_x() + bar.get_width()/2, bar.get_height() + 0.3,
                 f'{val:.1f}', ha='center', color='white', fontsize=8)

    # --- Graphique 4 : Distribution PM2.5 (histogram) ---
    ax4 = fig.add_subplot(gs[1, 1])
    ax4.set_facecolor(ax_bg)
    all_vals = list(avgs.values())
    n, bins, patches = ax4.hist(all_vals, bins=8,
                                color=bar_blue, alpha=0.8,
                                edgecolor='white', linewidth=0.5)
    ax4.axvline(12, color='#f39c12', linestyle='--',
                linewidth=2, label='Seuil OMS 12 µg/m³')
    ax4.set_xlabel('PM2.5 (µg/m³)', color='white')
    ax4.set_ylabel('Nb quartiers', color='white')
    ax4.set_title('Distribution PM2.5 tous quartiers',
                  color='white', fontsize=9)
    ax4.tick_params(colors='white')
    ax4.spines[:].set_color('#444')
    ax4.legend(fontsize=8, facecolor='#1a1a2e', labelcolor='white')

    plt.savefig(VIZ_OUTPUT, dpi=120, bbox_inches='tight',
                facecolor='#1a1a2e')
    plt.close()
    print(f"[{time.strftime('%H:%M:%S')}] Graphique mis à jour → "
          f"{VIZ_OUTPUT} ({nb_quartiers} quartiers, "
          f"avg global={global_avg:.2f} µg/m³)")

def main():
    print("=== Visualisation temps réel Chicago Air Quality ===")
    print(f"Actualisation toutes les {REFRESH_SECS} secondes")
    print(f"Lecture depuis HDFS : {OUTPUT_DIR}")
    print(f"Graphique sauvegardé : {VIZ_OUTPUT}")
    print("Ctrl+C pour arrêter\n")

    iteration = 0
    while True:
        try:
            data = get_latest_hdfs_results()
            iteration += 1
            make_plot(data, iteration)
            time.sleep(REFRESH_SECS)
        except KeyboardInterrupt:
            print("\nVisualisation arrêtée.")
            break

if __name__ == "__main__":
    main()
