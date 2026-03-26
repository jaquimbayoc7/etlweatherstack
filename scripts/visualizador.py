#!/usr/bin/env python3
"""
visualizador.py - Fase Visualize del pipeline ETL
Genera gráficas de análisis a partir de los datos de clima.
Soporta datasets de una fila por ciudad o series de tiempo (múltiples registros).
"""

import pandas as pd
import matplotlib.pyplot as plt
import matplotlib
import numpy as np
import os
import logging

matplotlib.use('Agg')  # Backend sin GUI para entornos headless

logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s',
    handlers=[
        logging.FileHandler('logs/etl.log'),
        logging.StreamHandler()
    ]
)
logger = logging.getLogger(__name__)

PALETA = ['#1e3c72', '#ff6b6b', '#4ecdc4', '#f39c12', '#27ae60']


def cargar_datos(path='data/clima.csv'):
    """Carga el CSV; intenta el transformado si existe"""
    transformado = path.replace('clima.csv', 'clima_transformado.csv')
    if os.path.exists(transformado):
        df = pd.read_csv(transformado)
        df['hora_local'] = pd.to_datetime(df['hora_local'], errors='coerce')
        logger.info(f"📂 Usando datos transformados: {transformado}")
    elif os.path.exists(path):
        df = pd.read_csv(path)
        df['hora_local'] = pd.to_datetime(df['hora_local'], errors='coerce')
        logger.info(f"📂 Usando datos crudos: {path}")
    else:
        raise FileNotFoundError(
            "No se encontró data/clima.csv ni data/clima_transformado.csv. "
            "Ejecuta primero scripts/extractor.py o demo_data.py"
        )
    return df


def es_serie_tiempo(df):
    """Retorna True si hay más de 1 registro por ciudad (serie de tiempo)"""
    return df.groupby('ciudad').size().max() > 1


# ── Gráficas para dataset simple (1 fila por ciudad) ─────────────────────────

def graficar_simple(df):
    fig, axes = plt.subplots(2, 2, figsize=(15, 10))
    fig.suptitle('Análisis de Clima por Ciudades Colombianas', fontsize=16, fontweight='bold')
    ciudades = df['ciudad']

    ax1 = axes[0, 0]
    bars = ax1.bar(ciudades, df['temperatura'], color='#ff6b6b', edgecolor='white')
    ax1.set_title('Temperatura Actual (°C)')
    ax1.set_ylabel('Temperatura (°C)')
    ax1.tick_params(axis='x', rotation=30)
    ax1.grid(axis='y', alpha=0.3)
    for bar, val in zip(bars, df['temperatura']):
        ax1.text(bar.get_x() + bar.get_width() / 2, bar.get_height() + 0.3,
                 f'{val}°', ha='center', va='bottom', fontsize=9)

    ax2 = axes[0, 1]
    bars2 = ax2.bar(ciudades, df['humedad'], color='#4ecdc4', edgecolor='white')
    ax2.set_title('Humedad Relativa (%)')
    ax2.set_ylabel('Humedad (%)')
    ax2.set_ylim(0, 110)
    ax2.tick_params(axis='x', rotation=30)
    ax2.grid(axis='y', alpha=0.3)
    for bar, val in zip(bars2, df['humedad']):
        ax2.text(bar.get_x() + bar.get_width() / 2, bar.get_height() + 1,
                 f'{val}%', ha='center', va='bottom', fontsize=9)

    ax3 = axes[1, 0]
    ax3.scatter(ciudades, df['velocidad_viento'], s=200, color='#95e1d3', edgecolors='#2a7873', zorder=5)
    ax3.plot(range(len(ciudades)), df['velocidad_viento'], color='#2a7873', linestyle='--', alpha=0.5)
    ax3.set_title('Velocidad del Viento (km/h)')
    ax3.set_ylabel('Velocidad (km/h)')
    ax3.tick_params(axis='x', rotation=30)
    ax3.grid(alpha=0.3)

    ax4 = axes[1, 1]
    x = np.arange(len(ciudades))
    width = 0.35
    ax4.bar(x - width / 2, df['temperatura'], width, label='Temperatura', color='#ff6b6b')
    ax4.bar(x + width / 2, df['sensacion_termica'], width, label='Sensación Térmica', color='#ffa07a')
    ax4.set_title('Temperatura vs Sensación Térmica (°C)')
    ax4.set_ylabel('Temperatura (°C)')
    ax4.set_xticks(x)
    ax4.set_xticklabels(ciudades, rotation=30)
    ax4.legend()
    ax4.grid(axis='y', alpha=0.3)

    plt.tight_layout()
    return fig


# ── Gráficas para series de tiempo (múltiples registros) ─────────────────────

def graficar_series(df):
    ciudades = sorted(df['ciudad'].unique())
    colores = {c: PALETA[i % len(PALETA)] for i, c in enumerate(ciudades)}

    plt.style.use('seaborn-v0_8-whitegrid')
    fig = plt.figure(figsize=(20, 18))
    fig.patch.set_facecolor('#f8f9fa')

    gs = fig.add_gridspec(3, 2, hspace=0.42, wspace=0.32,
                          left=0.07, right=0.97, top=0.93, bottom=0.05)

    fig.suptitle(
        f'Dashboard Climático — Colombia  |  {len(df):,} registros  |  {len(ciudades)} ciudades',
        fontsize=17, fontweight='bold', color='#1e3c72', y=0.97
    )

    # ── 1. Temperatura media diaria (líneas suavizadas) ──────────────────────
    ax1 = fig.add_subplot(gs[0, 0])
    if pd.api.types.is_datetime64_any_dtype(df['hora_local']):
        df_tmp = df.copy()
        df_tmp['fecha'] = df_tmp['hora_local'].dt.date
        diario = df_tmp.groupby(['fecha', 'ciudad'])['temperatura'].mean().reset_index()
        for ciudad in ciudades:
            sub = diario[diario['ciudad'] == ciudad].sort_values('fecha')
            ax1.plot(range(len(sub)), sub['temperatura'],
                     marker='o', markersize=5, linewidth=2,
                     label=ciudad, color=colores[ciudad])
            ax1.fill_between(range(len(sub)), sub['temperatura'],
                             alpha=0.08, color=colores[ciudad])
        # Etiquetas fecha cada 2 días
        sample = diario[diario['ciudad'] == ciudades[0]].sort_values('fecha')
        ticks = list(range(0, len(sample), max(1, len(sample) // 6)))
        ax1.set_xticks(ticks)
        ax1.set_xticklabels([str(sample.iloc[t]['fecha'])[5:] for t in ticks],
                             rotation=30, fontsize=8)
    ax1.set_title('Temperatura Media Diaria (°C)', fontweight='bold', pad=10)
    ax1.set_ylabel('°C')
    ax1.legend(fontsize=8, loc='upper right', framealpha=0.8)
    ax1.set_facecolor('white')

    # ── 2. Boxplot de distribución de temperatura ────────────────────────────
    ax2 = fig.add_subplot(gs[0, 1])
    data_box = [df[df['ciudad'] == c]['temperatura'].values for c in ciudades]
    bp = ax2.boxplot(data_box, patch_artist=True, tick_labels=ciudades,
                     widths=0.5, notch=False,
                     flierprops=dict(marker='o', markersize=3, alpha=0.3),
                     medianprops=dict(color='white', linewidth=2))
    for patch, ciudad in zip(bp['boxes'], ciudades):
        patch.set_facecolor(colores[ciudad])
        patch.set_alpha(0.75)
    for whisker in bp['whiskers']:
        whisker.set(linewidth=1.2, linestyle='--', alpha=0.6)
    ax2.set_title('Distribución de Temperatura por Ciudad (°C)', fontweight='bold', pad=10)
    ax2.set_ylabel('°C')
    ax2.tick_params(axis='x', rotation=25)
    ax2.set_facecolor('white')

    # ── 3. Barras comparativas: temp min / media / max ───────────────────────
    ax3 = fig.add_subplot(gs[1, 0])
    stats = df.groupby('ciudad')['temperatura'].agg(['min', 'mean', 'max']).reindex(ciudades)
    x = np.arange(len(ciudades))
    w = 0.25
    b1 = ax3.bar(x - w, stats['min'],  w, label='Mínima', color='#4ecdc4', edgecolor='white')
    b2 = ax3.bar(x,     stats['mean'], w, label='Media',   color='#ff6b6b', edgecolor='white')
    b3 = ax3.bar(x + w, stats['max'],  w, label='Máxima',  color='#f39c12', edgecolor='white')
    for bars in (b1, b2, b3):
        for bar in bars:
            h = bar.get_height()
            ax3.text(bar.get_x() + bar.get_width() / 2, h + 0.3,
                     f'{h:.0f}°', ha='center', va='bottom', fontsize=7.5)
    ax3.set_xticks(x)
    ax3.set_xticklabels(ciudades, rotation=25, fontsize=9)
    ax3.set_title('Temperatura Mín / Media / Máx por Ciudad (°C)', fontweight='bold', pad=10)
    ax3.set_ylabel('°C')
    ax3.legend(fontsize=8)
    ax3.set_facecolor('white')

    # ── 4. Scatter: Temperatura vs Humedad (con jitter de color por ciudad) ──
    ax4 = fig.add_subplot(gs[1, 1])
    for ciudad in ciudades:
        sub = df[df['ciudad'] == ciudad]
        ax4.scatter(sub['temperatura'], sub['humedad'],
                    alpha=0.35, s=18, color=colores[ciudad], label=ciudad)
    ax4.set_title('Temperatura vs Humedad', fontweight='bold', pad=10)
    ax4.set_xlabel('Temperatura (°C)')
    ax4.set_ylabel('Humedad (%)')
    ax4.legend(fontsize=8, markerscale=1.5)
    ax4.set_facecolor('white')

    # ── 5. Humedad media diaria ──────────────────────────────────────────────
    ax5 = fig.add_subplot(gs[2, 0])
    if pd.api.types.is_datetime64_any_dtype(df['hora_local']):
        df_tmp = df.copy()
        df_tmp['fecha'] = df_tmp['hora_local'].dt.date
        hum_diaria = df_tmp.groupby(['fecha', 'ciudad'])['humedad'].mean().reset_index()
        for ciudad in ciudades:
            sub = hum_diaria[hum_diaria['ciudad'] == ciudad].sort_values('fecha')
            ax5.plot(range(len(sub)), sub['humedad'],
                     linewidth=2, label=ciudad, color=colores[ciudad])
            ax5.fill_between(range(len(sub)), sub['humedad'],
                             alpha=0.08, color=colores[ciudad])
        sample = hum_diaria[hum_diaria['ciudad'] == ciudades[0]].sort_values('fecha')
        ticks = list(range(0, len(sample), max(1, len(sample) // 6)))
        ax5.set_xticks(ticks)
        ax5.set_xticklabels([str(sample.iloc[t]['fecha'])[5:] for t in ticks],
                             rotation=30, fontsize=8)
    ax5.set_title('Humedad Media Diaria (%)', fontweight='bold', pad=10)
    ax5.set_ylabel('%')
    ax5.legend(fontsize=8, loc='upper right', framealpha=0.8)
    ax5.set_facecolor('white')

    # ── 6. Frecuencia de condiciones climáticas (donut / barras horizontales) ─
    ax6 = fig.add_subplot(gs[2, 1])
    top_desc = df['descripcion'].value_counts().head(8)
    colors_desc = [plt.cm.Paired(i / len(top_desc)) for i in range(len(top_desc))]
    bars_h = ax6.barh(top_desc.index[::-1], top_desc.values[::-1],
                      color=colors_desc[::-1], edgecolor='white', height=0.6)
    for bar, val in zip(bars_h, top_desc.values[::-1]):
        ax6.text(val + 2, bar.get_y() + bar.get_height() / 2,
                 f'{val:,}', va='center', fontsize=9, fontweight='bold')
    ax6.set_title('Frecuencia de Condiciones Climáticas', fontweight='bold', pad=10)
    ax6.set_xlabel('Registros')
    ax6.set_xlim(0, top_desc.values.max() * 1.15)
    ax6.set_facecolor('white')

    plt.style.use('default')
    return fig


def graficar_analisis(df):
    """Selecciona el tipo de gráfica según el dataset y guarda el PNG"""
    os.makedirs('data', exist_ok=True)

    if es_serie_tiempo(df):
        logger.info(f"📈 Modo serie de tiempo detectado ({len(df):,} registros)")
        fig = graficar_series(df)
    else:
        logger.info("📊 Modo resumen por ciudad")
        fig = graficar_simple(df)

    output_path = 'data/clima_analysis.png'
    fig.savefig(output_path, dpi=150, bbox_inches='tight')
    logger.info(f"✅ Gráficas guardadas en {output_path}")
    plt.close(fig)
    return output_path


if __name__ == "__main__":
    try:
        df = cargar_datos()
        path = graficar_analisis(df)
        print(f"\n✅ Gráficas generadas en: {path}")
    except FileNotFoundError as e:
        logger.error(str(e))
    except Exception as e:
        logger.error(f"Error fatal en visualización: {str(e)}")
        raise
