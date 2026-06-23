#!/usr/bin/env python

import argparse
import geopandas as gpd
import pandas as pd
import matplotlib.pyplot as plt
import matplotlib.lines as mlines
import numpy as np


def class_accuracy(df, truth_col, area_col, true_vals, maybe_vals, n_boot, ci):
    truth = df[truth_col].values
    area = df[area_col].values.astype(float)
    n = len(truth)
    total_area = area.sum()
    true_set = set(true_vals)
    maybe_set = set(maybe_vals)

    true_mask = np.array([t in true_set for t in truth])
    lenient_mask = np.array([t in (true_set | maybe_set) for t in truth])

    p_strict = area[true_mask].sum() / total_area if total_area > 0 else 0.0
    p_lenient = area[lenient_mask].sum() / total_area if total_area > 0 else 0.0

    # Bootstrap CI: resample polygons with replacement, recompute area-weighted accuracy
    alpha = (1 - ci) / 2
    idx = np.random.randint(0, n, size=(n_boot, n))
    boot_area = area[idx]                        # (n_boot, n)
    boot_correct = true_mask[idx]                # (n_boot, n)
    boot_totals = boot_area.sum(axis=1)
    boot_p = np.where(boot_totals > 0,
                      (boot_area * boot_correct).sum(axis=1) / boot_totals,
                      0.0)
    ci_lo = float(np.percentile(boot_p, 100 * alpha))
    ci_hi = float(np.percentile(boot_p, 100 * (1 - alpha)))

    return pd.Series({'n': n, 'p_strict': p_strict, 'p_lenient': p_lenient,
                      'ci_lo': ci_lo, 'ci_hi': ci_hi})


def to_secondary_code(lu_code):
    parts = str(lu_code).split('.')
    if len(parts) == 3:
        parts[2] = '0'
        return '.'.join(parts)
    return lu_code


def _lu_sort_key(lu_code):
    try:
        return [int(x) for x in str(lu_code).split('.')]
    except ValueError:
        return [0]


def _cat_color(cat, true_vals, secondary_vals, maybe_vals):
    if cat in maybe_vals:
        return '#fdae61'
    if cat in secondary_vals:
        return '#56B4E9'
    if cat in true_vals:
        return '#0072B2'
    return '#d73027'


def _cat_sort_key(cat, true_vals, secondary_vals, maybe_vals):
    if cat not in true_vals and cat not in secondary_vals and cat not in maybe_vals:
        return 0
    if cat in maybe_vals:
        return 1
    if cat in secondary_vals:
        return 2
    return 3


def plot_calibration(ax, gdf, truth_col, confidence_col, area_col, true_vals, secondary_vals, maybe_vals):
    all_cats = gdf[truth_col].dropna().unique().tolist()
    cat_order = sorted(all_cats, key=lambda c: _cat_sort_key(c, true_vals, secondary_vals, maybe_vals))

    conf_vals = sorted(gdf[confidence_col].dropna().unique())

    total_per_conf = np.array([
        gdf.loc[gdf[confidence_col] == cv, area_col].sum()
        for cv in conf_vals
    ])
    grand_total = total_per_conf.sum()
    widths = np.log1p(total_per_conf) / np.log1p(total_per_conf).sum()
    lefts = np.concatenate([[0], np.cumsum(widths[:-1])])
    centers = lefts + widths / 2

    gap = 0.015
    bottom = np.zeros(len(conf_vals))

    for cat in cat_order:
        proportions = np.array([
            gdf.loc[(gdf[confidence_col] == cv) & (gdf[truth_col] == cat), area_col].sum() /
            max(1e-9, total_per_conf[i])
            for i, cv in enumerate(conf_vals)
        ])
        ax.bar(lefts, proportions, width=widths - gap, bottom=bottom, align='edge',
               color=_cat_color(cat, true_vals, secondary_vals, maybe_vals),
               label=str(cat))
        bottom += proportions

    ax.set_xticks(centers)
    ax.set_xticklabels([str(int(cv)) for cv in conf_vals])
    ax.set_xlabel("Confidence score (1 = most confident)\n(bar width ∝ log area in bin)")
    ax.set_ylabel("Proportion of bin area")
    ax.set_xlim(0, 1)
    ax.set_ylim(0, 1)
    ax.set_title("Validation outcome by confidence score")
    ax.legend(loc='upper left', fontsize=8, title="Outcome")
    ax.yaxis.grid(True, linestyle='--', linewidth=0.5, alpha=0.7)
    ax.set_axisbelow(True)


def _draw_accuracy_panel(ax, acc, threshold, title):
    y = np.arange(len(acc))

    dot_colors = [
        '#0072B2' if row['ci_lo'] > threshold else
        '#d73027' if row['ci_hi'] < threshold else
        '#fdae61'
        for _, row in acc.iterrows()
    ]

    for i, (_, row) in enumerate(acc.iterrows()):
        ax.plot([row['p_strict'], row['p_lenient']], [y[i], y[i]],
                color='#bbbbbb', linewidth=1.2, zorder=1)

    xerr = np.array([acc['p_strict'] - acc['ci_lo'], acc['ci_hi'] - acc['p_strict']])
    ax.errorbar(acc['p_strict'], y, xerr=xerr, fmt='none',
                color='#777777', linewidth=1, capsize=3, zorder=2)

    ax.scatter(acc['p_strict'], y, c=dot_colors, zorder=4, s=45)
    ax.scatter(acc['p_lenient'], y, facecolors='none', edgecolors='#555555',
               zorder=3, s=45, linewidths=1)

    ax.axvline(threshold, color='black', linestyle='--', linewidth=1)
    ax.text(threshold - 0.008, len(acc) - 0.7, f'{threshold:.0%}',
            fontsize=8, va='top', ha='right')

    ax.set_yticks(y)
    ax.set_yticklabels(acc.index.tolist(), fontsize=8)
    ax.set_xlabel("Area-weighted accuracy")
    ax.set_title(title)
    ax.xaxis.grid(True, linestyle='--', linewidth=0.5, alpha=0.7)
    ax.yaxis.grid(True, linestyle=':', linewidth=0.4, alpha=0.3, color='#888888')
    ax.set_axisbelow(True)
    ax.set_xlim(-0.02, 1.02)

    legend_items = [
        mlines.Line2D([], [], color='#0072B2', marker='o', linestyle='None', label='Passes'),
        mlines.Line2D([], [], color='#fdae61', marker='o', linestyle='None', label='Marginal'),
        mlines.Line2D([], [], color='#d73027', marker='o', linestyle='None', label='Fails'),
        mlines.Line2D([], [], marker='o', color='w', markerfacecolor='w',
                      markeredgecolor='#555555', linestyle='None', label='Lenient (incl. maybe)'),
    ]
    ax.legend(handles=legend_items, fontsize=8, loc='lower right')


def _groupby_accuracy(gdf, group_col, truth_col, area_col, true_vals, maybe_vals, n_boot, ci):
    rows = {key: class_accuracy(grp, truth_col, area_col, true_vals, maybe_vals, n_boot, ci)
            for key, grp in gdf.groupby(group_col)}
    return pd.DataFrame(rows).T


def plot_accuracy_uncombined(ax, gdf, lu_col, truth_col, area_col, true_vals, maybe_vals,
                             threshold, n_boot, ci):
    acc = _groupby_accuracy(gdf, lu_col, truth_col, area_col, true_vals, maybe_vals, n_boot, ci)
    acc = acc.loc[sorted(acc.index, key=_lu_sort_key, reverse=True)]
    _draw_accuracy_panel(ax, acc, threshold,
                         f"Tertiary accuracy\n(bootstrap {ci:.0%} CI)")


def plot_accuracy_combined(ax, gdf, lu_col, truth_col, area_col, true_vals, secondary_vals,
                           maybe_vals, threshold, n_boot, ci):
    gdf = gdf.copy()
    gdf['_secondary_lu'] = gdf[lu_col].map(to_secondary_code)
    strict_vals = list(set(true_vals) | set(secondary_vals))
    acc = _groupby_accuracy(gdf, '_secondary_lu', truth_col, area_col, strict_vals, maybe_vals, n_boot, ci)
    acc = acc.loc[sorted(acc.index, key=_lu_sort_key, reverse=True)]
    _draw_accuracy_panel(ax, acc, threshold,
                         f"Secondary accuracy\n(bootstrap {ci:.0%} CI)")


def main(gpkg_paths, truth_col, confidence_col, lu_col, area_col, true_values, secondary_values,
         maybe_values, threshold, n_boot, ci, output, include_null_class, null_class, a4):
    gdfs = [gpd.read_file(p) for p in gpkg_paths]

    if len(gdfs) > 1:
        ref_cols = set(gdfs[0].columns)
        ref_crs = gdfs[0].crs
        for path, g in zip(gpkg_paths[1:], gdfs[1:]):
            if set(g.columns) != ref_cols:
                extra = set(g.columns) - ref_cols
                missing = ref_cols - set(g.columns)
                raise ValueError(
                    f"Schema mismatch in {path}."
                    + (f" Extra columns: {extra}." if extra else "")
                    + (f" Missing columns: {missing}." if missing else "")
                )
            if g.crs != ref_crs:
                raise ValueError(f"CRS mismatch in {path}: expected {ref_crs}, got {g.crs}")
        gdf = gpd.GeoDataFrame(pd.concat(gdfs, ignore_index=True), crs=ref_crs)
    else:
        gdf = gdfs[0]

    for col in [lu_col, confidence_col, truth_col]:
        if col not in gdf.columns:
            raise ValueError(f"Column '{col}' not found in the dataset")

    if area_col:
        if area_col not in gdf.columns:
            raise ValueError(f"Column '{area_col}' not found in the dataset")
        gdf['_area'] = gdf[area_col]
    else:
        gdf['_area'] = gdf.geometry.area
    area_col = '_area'

    if not include_null_class:
        null_cls = null_class or sorted(gdf[lu_col].unique(), key=_lu_sort_key)[0]
        gdf = gdf[gdf[lu_col] != null_cls]

    n_tertiary = gdf[lu_col].nunique()
    n_secondary = gdf[lu_col].map(to_secondary_code).nunique()

    if a4:
        figsize = (11.69, 8.27)  # A4 landscape in inches
        dpi = 300
    else:
        fig_h = max(6, max(n_tertiary, n_secondary) * 0.35 + 2)
        figsize = (24, fig_h)
        dpi = 150

    fig, (ax1, ax2, ax3) = plt.subplots(1, 3, figsize=figsize)

    plot_calibration(ax1, gdf, truth_col, confidence_col, area_col, true_values, secondary_values, maybe_values)
    plot_accuracy_uncombined(ax2, gdf, lu_col, truth_col, area_col, true_values, maybe_values, threshold, n_boot, ci)
    plot_accuracy_combined(ax3, gdf, lu_col, truth_col, area_col, true_values, secondary_values, maybe_values, threshold, n_boot, ci)

    plt.tight_layout()

    if output:
        plt.savefig(output, dpi=dpi, bbox_inches='tight')
        print(f"Saved to {output}")
    else:
        plt.show()


if __name__ == "__main__":
    parser = argparse.ArgumentParser(
        description="Three-panel validation summary: confidence calibration, tertiary accuracy, secondary (combined) accuracy."
    )
    parser.add_argument("gpkg", nargs="+", help="Path(s) to input GeoPackage file(s). Multiple files are concatenated; schemas and CRS must match.")
    parser.add_argument("--truth-col", default="validation",
                        help="Validation attribute column (default: validation)")
    parser.add_argument("--confidence-col", default="confidence",
                        help="Confidence score column (default: confidence)")
    parser.add_argument("--lu-col", default="lu_code",
                        help="Land use code column (default: lu_code)")
    parser.add_argument("--true-values", nargs="+", default=["trueTertiary"],
                        metavar="VALUE",
                        help="Values counted as strict-correct at tertiary level (default: trueTertiary)")
    parser.add_argument("--secondary-values", nargs="+", default=["trueSecondary"],
                        metavar="VALUE",
                        help="Values additionally counted as strict-correct in combined/secondary view (default: trueSecondary)")
    parser.add_argument("--maybe-values", nargs="+", default=["maybe"],
                        metavar="VALUE",
                        help="Values counted as maybe-correct (lenient only) (default: maybe)")
    parser.add_argument("--threshold", type=float, default=0.8,
                        help="Accuracy threshold line (default: 0.8)")
    parser.add_argument("--bootstrap-n", type=int, default=1000,
                        help="Number of bootstrap resamples (default: 1000)")
    parser.add_argument("--ci", type=float, default=0.95,
                        help="CI level (default: 0.95)")
    parser.add_argument("--output", default=None,
                        help="Save to file (PNG, PDF, SVG…). Omit to show interactively.")
    parser.add_argument("--area-col", default=None,
                        help="Column to use as polygon area (default: computed from geometry)")
    parser.add_argument("--a4", action="store_true",
                        help="Constrain output to A4 landscape (11.69x8.27in, 300dpi). May be cramped with many classes.")
    parser.add_argument("--include-null-class", action="store_true",
                        help="Include the null/unclassified class (default: omit)")
    parser.add_argument("--null-class", default=None,
                        help="Null class code to omit (default: first lu_code in sort order)")
    args = parser.parse_args()

    main(
        args.gpkg, # list
        truth_col=args.truth_col,
        confidence_col=args.confidence_col,
        lu_col=args.lu_col,
        area_col=args.area_col,
        true_values=args.true_values,
        secondary_values=args.secondary_values,
        maybe_values=args.maybe_values,
        threshold=args.threshold,
        n_boot=args.bootstrap_n,
        ci=args.ci,
        output=args.output,
        include_null_class=args.include_null_class,
        null_class=args.null_class,
        a4=args.a4,
    )
