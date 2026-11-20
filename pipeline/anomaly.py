"""Sequential models for the edit-rate time series (the bonus: anomaly detection on a stream).

The series is "number of edits per 10-second bucket". Counts are roughly Poisson, so we first apply the
Anscombe variance-stabilising transform  y = 2*sqrt(x + 3/8)  (for Poisson data Var(y) ~ 1 at any rate).
Then two online (one-step-ahead) forecasters are compared:

* EwmaModel  – exponentially weighted moving average level (baseline).
* RlsArModel – autoregressive model AR(p) with a bias, trained *online* by recursive least squares
               with exponential forgetting (adapts to daily rhythm without ever re-training).

For every new bucket the model first forecasts, then sees the truth, then updates. The anomaly score is the
standardised forecast residual  z = (y - forecast) / max(sd, sd_floor)  (sd = running RMS of past residuals).
"""
import math
from collections import deque

import numpy as np


def anscombe(x: float) -> float:
    return 2.0 * math.sqrt(x + 0.375)


def inv_anscombe(y: float) -> float:
    return max((y / 2.0) ** 2 - 0.375, 0.0)


SD_FLOOR = 0.5      # Poisson counts give sd~1 after the Anscombe transform; real streams are smoother, so 0.5 is the floor


class EwmaModel:
    def __init__(self, alpha: float = 0.1, sd_floor: float = SD_FLOOR):
        self.alpha = alpha
        self.sd_floor = sd_floor
        self.level: float | None = None
        self.s2 = 1.0

    def update(self, x: float) -> tuple[float, float]:
        """Returns (forecast_in_counts, z) for the observation x, then learns from it."""
        y = anscombe(x)
        if self.level is None:
            self.level = y
            return x, 0.0
        err = y - self.level
        z = err / max(math.sqrt(self.s2), self.sd_floor)
        self.s2 = 0.95 * self.s2 + 0.05 * err * err
        forecast = inv_anscombe(self.level)
        self.level += self.alpha * err
        return forecast, z


class RlsArModel:
    def __init__(self, order: int = 3, forgetting: float = 0.995, delta: float = 100.0, sd_floor: float = SD_FLOOR):
        self.p = order
        self.sd_floor = sd_floor
        self.lam = forgetting
        self.w = np.zeros(order + 1)
        self.P = np.eye(order + 1) * delta
        self.hist: deque[float] = deque(maxlen=order)
        self.s2 = 1.0
        self.n = 0

    def _phi(self) -> np.ndarray:
        lags = list(self.hist)[::-1]                      # most recent first
        return np.array(lags + [1.0])

    def update(self, x: float) -> tuple[float, float]:
        y = anscombe(x)
        self.n += 1
        if len(self.hist) < self.p:                       # not enough history yet
            self.hist.append(y)
            return x, 0.0
        phi = self._phi()
        pred = float(self.w @ phi)
        err = y - pred
        z = err / max(math.sqrt(self.s2), self.sd_floor)
        # --- learn (recursive least squares with forgetting)
        Pphi = self.P @ phi
        k = Pphi / (self.lam + phi @ Pphi)
        self.w = self.w + k * err
        self.P = (self.P - np.outer(k, Pphi)) / self.lam
        np.clip(self.P, -1e4, 1e4, out=self.P)            # numerical safety
        self.s2 = 0.97 * self.s2 + 0.03 * err * err
        self.hist.append(y)
        return inv_anscombe(pred), z


class SeriesScorer:
    """Both models for one series, plus warm-up bookkeeping."""

    def __init__(self, warmup: int = 18, sd_floor: float = SD_FLOOR):
        self.ewma, self.ar = EwmaModel(sd_floor=sd_floor), RlsArModel(sd_floor=sd_floor)
        self.warmup = warmup
        self.n = 0

    def score(self, x: int) -> dict:
        f_ar, z_ar = self.ar.update(x)
        _, z_ewma = self.ewma.update(x)
        self.n += 1
        ready = self.n > self.warmup
        return {"forecast": f_ar if ready else None, "z_ar": z_ar if ready else 0.0, "z_ewma": z_ewma if ready else 0.0, "ready": ready}
