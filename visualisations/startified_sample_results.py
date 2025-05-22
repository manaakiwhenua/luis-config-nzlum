#!/usr/bin/env python

import sys
import argparse
import geopandas as gpd
import pandas as pd
import matplotlib.pyplot as plt
import numpy as np
import matplotlib.cm as cm

def main(gpkg_path):
    # Read data
    gdf = gpd.read_file(gpkg_path)

    # Validate expected columns
    for col in ['lu_code', 'confidence', 'truth']:
        if col not in gdf.columns:
            raise ValueError(f"Column '{col}' not found in the dataset")

    # # Group and count
    counts = gdf.groupby(['lu_code', 'truth', 'confidence']).size().reset_index(name='count')

    # Total per lu_code x truth
    # counts['total'] = counts.groupby(['lu_code', 'truth'])['count'].transform('sum')
    # counts['proportion'] = counts['count'] / counts['total']

    # Total per lu_code (ignoring truth)
    total_per_lu = counts.groupby('lu_code')['count'].transform('sum')
    counts['proportion'] = counts['count'] / total_per_lu

    # Pivot table
    pivot_df = counts.pivot_table(index=['lu_code', 'truth'],
                                  columns='confidence',
                                  values='proportion',
                                  fill_value=0).reset_index()

    # Plotting setup
    cmap = cm.get_cmap('RdGy_r', 12)
    colors = [cmap(i) for i in range(12)]
    fig, ax = plt.subplots(figsize=(12, 6))

    lu_codes = sorted(gdf['lu_code'].unique())[1:] # Exclude null class
    truth_values = ['Yes','Maybe','No']
    width = 0.8 / len(truth_values)
    offsets = np.linspace(-0.4 + width/2, 0.4 - width/2, len(truth_values))


    for i, truth_val in enumerate(truth_values):
        subset = pivot_df[pivot_df['truth'] == truth_val]
        all_lu_codes_df = pd.DataFrame({'lu_code': sorted(lu_codes), 'truth': truth_val})
        subset = all_lu_codes_df.merge(subset, on='lu_code', how='left').fillna(0.0)
        x = np.arange(len(subset))
        bottom = np.zeros(len(subset))

        for conf in range(1, 13):
            heights = subset.get(conf, 0)
            ax.bar(x + offsets[i], heights, bottom=bottom, width=width,
                   color=colors[conf - 1], label=str(conf) if i == 0 else "")
            bottom += heights

        for j, lu in enumerate(subset['lu_code']):
            ax.text(x[j] + offsets[i], 1.02, truth_val[0].upper(), ha='center', fontsize=5, rotation=0)

    ax.set_xticks(np.arange(len(lu_codes)))
    ax.set_xticklabels(lu_codes, rotation=45, ha='right')
    ax.set_ylabel("Proportion of validation sample per lu_code")
    ax.set_xlabel("Land use code (lu_code)")
    # ax.set_title("Stacked Confidence by Land Use and Truth Label")
    
    # Add gridlines to y-axis
    ax.yaxis.grid(True, linestyle='--', linewidth=0.5, alpha=0.7)
    ax.set_axisbelow(True)

    ax.legend(title="Confidence", bbox_to_anchor=(1.05, 1), loc='upper left')
    plt.tight_layout()
    plt.show()


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Plot confidence distribution by land use code and truth labels.")
    parser.add_argument("gpkg", help="Path to the input GeoPackage file")
    args = parser.parse_args()
    main(args.gpkg)

