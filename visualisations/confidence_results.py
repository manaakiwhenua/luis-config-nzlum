#!/usr/bin/env python

import sys
import argparse
import geopandas as gpd
import pandas as pd
import matplotlib.pyplot as plt
import numpy as np
import matplotlib.cm as cm

CMAP = 'RdGy_r'

def bin_confidence(conf):
    return (int(conf) - 1) // 3 + 1  # Bin 1 to 4

def main(gpkg_path, mode, log_scale):
    gdf = gpd.read_file(gpkg_path)
    gdf = gdf.dropna(subset=['lu_code','confidence'])

    for col in ['lu_code', 'confidence']:
        if col not in gdf.columns:
            raise ValueError(f"Column '{col}' not found in the dataset")

    if mode == 'area':
        if gdf.geometry.is_empty.any():
            raise ValueError("Some geometries are empty.")
        gdf['value'] = gdf.geometry.area / 10_000 # m2 to ha
    else:
        gdf['value'] = 1

    if log_scale and mode == 'area':
        gdf['conf_bin'] = gdf['confidence'].apply(bin_confidence)
        counts = gdf.groupby(['lu_code', 'conf_bin'])['value'].sum().reset_index()
        pivot_df = counts.pivot(index='lu_code', columns='conf_bin', values='value').fillna(0)
        pivot_df = pivot_df.loc[sorted(pivot_df.index)]

        # Plot: grouped bars
        fig, ax = plt.subplots(figsize=(12, 6))
        x = np.arange(len(pivot_df))
        width = 0.8 / 4
        offsets = np.linspace(-0.4 + width/2, 0.4 - width/2, 4)
        # Get 4 evenly spaced colors from the colormap
        cmap = cm.get_cmap(CMAP, 4)
        colors = [cmap(i) for i in range(cmap.N)]

        for i, bin_num in enumerate(range(1, 5)):
            values = pivot_df.get(bin_num, pd.Series(0, index=pivot_df.index))
            ax.bar(x + offsets[i], values, width=width, color=colors[i], label=f"{3*bin_num - 2}–{3*bin_num}")

        ax.set_yscale('log')
        ax.set_ylabel("Area (hectares, log scale)")
        ax.set_xticks(x)
        ax.set_xticklabels(pivot_df.index, rotation=45, ha='right')
        ax.set_xlabel("Land use code (lu_code)")
        ax.legend(title="Confidence bin")
        ax.grid(True, axis='y', linestyle='--', alpha=0.5)
        plt.tight_layout()
        plt.show()
        return

    # Continue with stacked plotting for other cases
    counts = gdf.groupby(['lu_code', 'confidence'])['value'].sum().reset_index()

    if mode == 'proportion':
        total_by_lu = counts.groupby('lu_code')['value'].transform('sum')
        counts['value'] = counts['value'] / total_by_lu

    pivot_df = counts.pivot(index='lu_code', columns='confidence', values='value').fillna(0)
    pivot_df = pivot_df.loc[sorted(pivot_df.index)]

    # Plotting setup
    cmap = cm.get_cmap(CMAP, 12)
    colors = [cmap(i) for i in range(12)]

    fig, ax = plt.subplots(figsize=(12, 6))
    bottom = np.zeros(len(pivot_df))
    x = np.arange(len(pivot_df))

    for conf in range(1, 13):
        values = pivot_df.get(conf, pd.Series(0, index=pivot_df.index))
        ax.bar(x, values, bottom=bottom, color=colors[conf - 1], label=str(conf))
        bottom += values

    ax.set_xticks(x)
    ax.set_xticklabels(pivot_df.index, rotation=45, ha='right')
    ax.set_xlabel("Land use code (lu_code)")
    if mode == 'area':
        ax.set_ylabel("Area (hectares)")
        ticks = ax.get_yticks()
        # ax.set_yticklabels([f"{tick:.1f}" for tick in ticks])
    else:
        ax.set_ylabel("Proportion")

    ax.legend(title="Confidence", bbox_to_anchor=(1.05, 1), loc='upper left')
    ax.grid(True, axis='y', linestyle='--', alpha=0.5)
    plt.tight_layout()
    plt.show()


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Plot confidence distribution by land use code.")
    parser.add_argument("gpkg", help="Path to the input GeoPackage file")
    parser.add_argument("--by", choices=["proportion", "area"], default="proportion",
                        help="Whether to plot by proportion or geographic area")
    parser.add_argument("--log", action="store_true",
                        help="Use log scale for area mode (switches to unstacked bars)")
    args = parser.parse_args()
    main(args.gpkg, args.by, args.log)
