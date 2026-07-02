import numpy as np
import pandas as pd
from scipy import stats
import matplotlib.pyplot as plt


def _abs_sorted(x):
    return np.sort(np.abs(x[x != 0]))[::-1]


def hill_estimator(data, k=None):
    """Hill estimator for tail index alpha. Lower alpha = heavier tail."""
    x = _abs_sorted(data)
    n = len(x)
    k = min(k or max(10, int(0.05 * n)), n - 1)
    alpha = 1 / np.mean(np.log(x[:k] / x[k]))
    return alpha, k


def hill_plot(x, name="feature", k_min=10, k_max_frac=0.5, ax=None):
    """Plot Hill alpha vs k; look for a plateau as the best estimate."""
    x_sorted = _abs_sorted(np.asarray(x))
    n = len(x_sorted)
    k_max = max(int(k_max_frac * n), k_min + 1)

    ks = np.arange(k_min, k_max)
    log_x = np.log(x_sorted)
    alphas = np.array([1 / np.mean(log_x[:k] - log_x[k]) for k in ks])

    ax = ax or plt.subplots(figsize=(6, 4))[1]
    ax.plot(ks, alphas)
    ax.axhline(2, color='red', linestyle='--', alpha=0.5, label='alpha=2 (infinite variance below)')
    ax.axhline(4, color='orange', linestyle='--', alpha=0.5, label='alpha=4 (heavy-tail rule of thumb)')
    ax.set_xlabel("k (number of top order statistics)")
    ax.set_ylabel("Hill alpha estimate")
    ax.set_title(f"{name}: Hill Plot")
    ax.legend(fontsize=8)
    return ks, alphas


def analyze_column(x, name="feature"):
    x = np.asarray(x)
    x = x[~np.isnan(x)]
    if len(x) < 30:
        print(f"{name}: too few points, skipping")
        return None

    kurt, skew = stats.kurtosis(x, fisher=True), stats.skew(x)
    alpha, k = hill_estimator(x)
    heavy_by_kurtosis, heavy_by_hill = kurt > 1.0, alpha < 4

    verdict = ("HEAVY-TAILED" if heavy_by_kurtosis and heavy_by_hill else
               "POSSIBLY HEAVY-TAILED" if heavy_by_kurtosis or heavy_by_hill else
               "LIGHT-TAILED")

    print(f"\n--- {name} ---")
    print(f"n={len(x)}, skew={skew:.2f}, excess_kurtosis={kurt:.2f}")
    print(f"Hill tail index alpha={alpha:.2f} (using top {k} points)")
    print(f"  alpha<2: infinite variance | 2<alpha<4: heavy but finite variance | alpha>4: light-ish")
    print(f"Verdict: {verdict}")

    return {"name": name, "kurtosis": kurt, "skew": skew, "hill_alpha": alpha, "verdict": verdict}


def plot_tail_comparison(x, name="feature", ax=None):
    """
    Overlay the empirical survival function (CCDF) against a fitted Gaussian's
    CCDF on log-log axes. A heavy tail shows up as the empirical curve staying
    high (roughly a straight line) while the Gaussian curve plunges to zero.
    """
    x_pos = _abs_sorted(x)
    n = len(x_pos)
    survival = np.arange(1, n + 1) / n

    mu, sigma = np.mean(x), np.std(x)
    gaussian_survival = 2 * stats.norm.sf(x_pos, loc=abs(mu), scale=sigma)

    ax = ax or plt.subplots(figsize=(6, 4))[1]
    ax.loglog(x_pos, survival, marker='.', linestyle='none', label='empirical data')
    ax.loglog(x_pos, gaussian_survival, color='red', linestyle='--', label='fitted normal')
    ax.set_xlabel("x")
    ax.set_ylabel("P(X > x)")
    ax.set_title(f"{name}: Tail comparison (empirical vs normal)")
    ax.legend(fontsize=8)
    if ax.figure.axes == [ax]:
        plt.tight_layout()
        plt.show()
    return ax


def plot_diagnostics(x, name="feature"):
    x = np.asarray(x)
    x = x[~np.isnan(x)]
    fig, axes = plt.subplots(1, 5, figsize=(25, 4))

    axes[0].hist(x, bins=50, density=True, alpha=0.7)
    axes[0].set_title(f"{name}: Histogram")

    stats.probplot(x, dist="norm", plot=axes[1])
    axes[1].set_title(f"{name}: QQ vs Normal")

    x_pos = _abs_sorted(x)
    axes[2].loglog(x_pos, np.arange(1, len(x_pos) + 1) / len(x_pos), marker='.', linestyle='none')
    axes[2].set_title(f"{name}: Log-log survival")
    axes[2].set_xlabel("x")
    axes[2].set_ylabel("P(X > x)")

    hill_plot(x, name=name, ax=axes[3])

    plot_tail_comparison(x, name=name, ax=axes[4])

    plt.tight_layout()
    plt.show()


def check_dataset_heavy_tails(df, feature=None, plot=True):
    """
    Run heavy-tail diagnostics on numeric column(s) of a DataFrame.

    Parameters:
        df : pandas DataFrame
        feature : str or None -- if given, only analyze this column; else all numeric columns
        plot : bool | "tail" -- True for full diagnostics, "tail" for just the
            empirical-vs-normal tail comparison, False for no plots
    """
    if feature is not None:
        if feature not in df.columns:
            raise ValueError(f"'{feature}' not found in DataFrame columns: {list(df.columns)}")
        cols = [feature]
    else:
        cols = df.select_dtypes(include=[np.number]).columns

    results = []
    for col in cols:
        res = analyze_column(df[col].values, name=col)
        if res:
            results.append(res)
            if plot == "tail":
                x = df[col].values
                plot_tail_comparison(x[~np.isnan(x)], name=col)
            elif plot:
                plot_diagnostics(df[col].values, name=col)

    return pd.DataFrame(results)

# ---- USAGE ----
df = pd.read_csv("datasets/tweet_frequency_summary.csv")
# summary = check_dataset_heavy_tails(df, plot=True)                          # all numeric columns
summary = check_dataset_heavy_tails(df, feature="tweet_count", plot="tail")   # just the tail-comparison plot
print(summary)
