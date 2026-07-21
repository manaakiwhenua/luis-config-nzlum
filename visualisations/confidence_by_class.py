#!/usr/bin/env python

import argparse
import geopandas as gpd
import pandas as pd
import numpy as np
import matplotlib.pyplot as plt
import matplotlib.patches as mpatches

import validation_accuracy as va


GROUP_COLORS = {
    "Correct": "#0072B2",
    "Maybe": "#fdae61",
    "False": "#d73027",
}
GROUP_ORDER = ["Correct", "Maybe", "False"]
GROUP_OFFSETS = {"Correct": 0.26, "Maybe": 0.0, "False": -0.26}
BOX_H = 0.22


def classify(truth, true_set, maybe_set):
    if truth in true_set:
        return "Correct"
    if truth in maybe_set:
        return "Maybe"
    return "False"


def main(gpkg_paths, truth_col, confidence_col, lu_col, true_values, secondary_values,
         maybe_values, level, min_n, output, a4):
    gdfs = [gpd.read_file(p) for p in gpkg_paths]
    ref_crs = gdfs[0].crs
    gdf = gpd.GeoDataFrame(pd.concat(gdfs, ignore_index=True), crs=ref_crs) if len(gdfs) > 1 else gdfs[0]

    for col in [lu_col, confidence_col, truth_col]:
        if col not in gdf.columns:
            raise ValueError(f"Column '{col}' not found in the dataset")

    null_cls = sorted(gdf[lu_col].unique(), key=va._lu_sort_key)[0]
    gdf = gdf[gdf[lu_col] != null_cls]

    gdf = gdf.copy()
    if level == "secondary":
        gdf["_class"] = gdf[lu_col].map(va.to_secondary_code)
        true_set = set(true_values) | set(secondary_values)
    else:
        gdf["_class"] = gdf[lu_col]
        true_set = set(true_values)
    maybe_set = set(maybe_values)

    gdf["_group"] = gdf[truth_col].apply(lambda t: classify(t, true_set, maybe_set))

    class_counts = gdf["_class"].value_counts()
    keep_classes = class_counts[class_counts >= min_n].index
    gdf = gdf[gdf["_class"].isin(keep_classes)]

    classes = sorted(gdf["_class"].unique(), key=va._lu_sort_key, reverse=True)
    n_classes = len(classes)

    if a4:
        figsize = (8.27, 11.69)
        dpi = 300
    else:
        figsize = (10, max(6, n_classes * 0.62 + 1))
        dpi = 150

    fig, ax = plt.subplots(figsize=figsize, dpi=dpi)

    for i in range(1, n_classes):
        ax.axhline(i - 0.5, color='#bbbbbb', linewidth=0.6, linestyle='-', alpha=0.5, zorder=0)

    for i, cls in enumerate(classes):
        sub = gdf[gdf["_class"] == cls]
        for grp in GROUP_ORDER:
            vals = sub.loc[sub["_group"] == grp, confidence_col].dropna().values
            if len(vals) == 0:
                continue
            pos = i + GROUP_OFFSETS[grp]
            bp = ax.boxplot(
                [vals], positions=[pos], vert=False, widths=BOX_H,
                patch_artist=True, showfliers=False, whis=(5, 95),
                manage_ticks=False,
            )
            for box in bp['boxes']:
                box.set_facecolor(GROUP_COLORS[grp])
                box.set_alpha(0.75)
                box.set_edgecolor(GROUP_COLORS[grp])
            for element in ('whiskers', 'caps'):
                for line in bp[element]:
                    line.set_color(GROUP_COLORS[grp])
            for median in bp['medians']:
                median.set_color('white')
                median.set_linewidth(1.5)
            ax.scatter(vals.mean(), pos, marker='D', s=10, facecolor='white',
                       edgecolor=GROUP_COLORS[grp], linewidths=0.8, zorder=5)
        ax.text(12.7, i, f"n={len(sub)}", fontsize=6, va='center', color='#555555')

    ax.set_yticks(range(n_classes))
    ax.set_yticklabels(classes, fontsize=8)
    ax.set_xlim(0.3, 13.3)
    ax.set_xticks(range(1, 13))
    ax.set_xlabel("More ⟷ less confident")
    ax.set_ylabel(f"NZLUM v0.4 {level} class")
    ax.set_title(f"Confidence score distribution by outcome, per {level} class")
    ax.xaxis.grid(True, linestyle='--', linewidth=0.5, alpha=0.7)
    ax.set_axisbelow(True)

    legend_handles = [mpatches.Patch(color=GROUP_COLORS[g], label=g, alpha=0.75) for g in GROUP_ORDER]
    legend_handles.append(
        plt.Line2D([], [], marker='D', color='w', markerfacecolor='white',
                   markeredgecolor='#555555', linestyle='None', markersize=5, label='Mean')
    )
    ax.legend(handles=legend_handles, loc='upper right', fontsize=8, title="Outcome",
              bbox_to_anchor=(1.16, 1.0), bbox_transform=ax.transAxes)

    plt.tight_layout()

    if output:
        plt.savefig(output, dpi=dpi, bbox_inches='tight')
        print(f"Saved to {output}")
    else:
        plt.show()


if __name__ == "__main__":
    parser = argparse.ArgumentParser(
        description="Per-class confidence score distribution, split by validation outcome (Correct/Maybe/False)."
    )
    parser.add_argument("gpkg", nargs="+", help="Path(s) to input GeoPackage file(s). Multiple files are concatenated.")
    parser.add_argument("--truth-col", default="validation")
    parser.add_argument("--confidence-col", default="confidence")
    parser.add_argument("--lu-col", default="lu_code")
    parser.add_argument("--true-values", nargs="+", default=["trueTertiary"], metavar="VALUE")
    parser.add_argument("--secondary-values", nargs="+", default=["trueSecondary"], metavar="VALUE")
    parser.add_argument("--maybe-values", nargs="+", default=["maybe"], metavar="VALUE")
    parser.add_argument("--level", choices=["tertiary", "secondary"], default="secondary",
                        help="Class granularity (default: secondary)")
    parser.add_argument("--min-n", type=int, default=10,
                        help="Minimum samples for a class to be shown (default: 10)")
    parser.add_argument("--output", default=None, help="Save to file (PNG, PDF, SVG...). Omit to show interactively.")
    parser.add_argument("--a4", action="store_true", help="Constrain output to A4 portrait (8.27x11.69in, 300dpi).")
    args = parser.parse_args()

    main(
        args.gpkg,
        truth_col=args.truth_col,
        confidence_col=args.confidence_col,
        lu_col=args.lu_col,
        true_values=args.true_values,
        secondary_values=args.secondary_values,
        maybe_values=args.maybe_values,
        level=args.level,
        min_n=args.min_n,
        output=args.output,
        a4=args.a4,
    )
