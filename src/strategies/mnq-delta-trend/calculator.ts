// calculator.ts — Dec26 Claude - Fixed code with session reset, breakout tolerance, EMA tolerance
import { BarData, MarketState, StrategyConfig, TradeSignal } from './types';
import { TechnicalCalculator } from '../../utils/technical';

console.log('[calc] loaded', __filename);

export class MNQDeltaTrendCalculator {
  private config: StrategyConfig;
  private technical: TechnicalCalculator;

  // Closed bars storage
  public bars3min: BarData[] = [];
  public bars15min: BarData[] = [];
  private isWarmUpProcessed = false;

  // Position / trailing
private currentPosition: {
    entryPrice: number;
    entryTime: number;
    direction: 'long' | 'short';
    stopLoss: number;
    atrSeedForStop: number;
    atrSeedForTrail: number;
  } | null = null;

  private trailingStopLevel = 0;
  private trailArmed = false;
  private noTrailBeforeMs = 0;

  // Track HTF bucket
  public lastHTFBucketStartMs: number | null = null;

  // Intra-bar signal tracking
  private intraBarDeltaHistory: Array<{ delta: number; timestamp: number }> = [];
  private lastIntraBarSignalTime = 0;
  private lastEntryBarTimestamp: string | null = null;

  public pushIntraBarDelta(delta: number, timestamp: number): void {
    this.intraBarDeltaHistory.push({ delta, timestamp });
  }

  public getIntraBarDeltaHistory(): Array<{ delta: number; timestamp: number }> {
    return this.intraBarDeltaHistory;
  }

  // ATR snapshot for entry
  private atrAtSignal: number = 0;

  constructor(config: StrategyConfig) {
    this.config = config;
    this.technical = new TechnicalCalculator();
    console.info('[MNQDeltaTrend][Config:Calculator]', this.config);
  }

  public getConfig(): Readonly<StrategyConfig> {
    return this.config;
  }

public resetState(): void {
    // Preserve warmup data (bars3min, bars15min, isWarmUpProcessed) - loaded by strategy.ts
    this.lastHTFBucketStartMs = null;
    this.intraBarDeltaHistory = [];
    this.lastIntraBarSignalTime = 0;
    this.lastEntryBarTimestamp = null;
    this.currentPosition = null;
    this.trailingStopLevel = 0;
    this.trailArmed = false;
    this.atrAtSignal = 0;
  }

  private isInTradingSession(timestamp: string): boolean {
    try {
      const tz = 'America/New_York';
      const barTime = new Date(timestamp);
      const options: Intl.DateTimeFormatOptions = { hour: '2-digit', minute: '2-digit', hour12: false, timeZone: tz };
      const hhmm = new Intl.DateTimeFormat('en-US', options).format(barTime);
      const [h, m] = hhmm.split(':').map(n => parseInt(n, 10));
      const currentMinutes = h * 60 + m;
      const [sh, sm] = (this.config.tradingStartTime ?? '09:30').split(':').map(n => parseInt(n, 10));
      const [eh, em] = (this.config.tradingEndTime ?? '15:55').split(':').map(n => parseInt(n, 10));
      const startMinutes = sh * 60 + sm;
      const endMinutes = eh * 60 + em;
      return currentMinutes >= startMinutes && currentMinutes <= endMinutes;
    } catch (err) {
      console.warn('[MNQDeltaTrend][SessionGate] error:', err);
      return false;
    }
  }

  // === WARMUP ===
  processWarmUpBar(bar: BarData, timeframe: '3min' | 'HTF'): void {
    const arr = timeframe === '3min' ? this.bars3min : this.bars15min;
    const prevClose = arr.length ? arr[arr.length - 1].close : NaN;
    const vol = Number.isFinite(bar.volume as any) ? Number(bar.volume) : 0;
    const signedVol =
      Number.isFinite(prevClose) && Number.isFinite(bar.close)
        ? (bar.close > prevClose ? vol : bar.close < prevClose ? -vol : 0)
        : 0;

    const normalized: BarData = {
      ...bar,
      delta: (typeof bar.delta === 'number' && Number.isFinite(bar.delta))
        ? Math.trunc(bar.delta)
        : Math.trunc(signedVol),
    };
    arr.push(normalized);
    if (timeframe === '3min' && this.bars3min.length > 2000) this.bars3min.shift();
    if (timeframe === 'HTF' && this.bars15min.length > 1000) this.bars15min.shift();
  }

  completeWarmUp(): void {
    this.isWarmUpProcessed = true;
    console.info(`[MNQDeltaTrend][warmup] complete - bars3min=${this.bars3min.length} bars15min=${this.bars15min.length}`);
    if (this.bars3min.length > 0 || this.bars15min.length > 0) {
      void this.calculateATR();
      void this.determineTrend();
    }
  }

  // === BAR PROCESSING ===
  processNewBar(incoming: BarData, marketState: MarketState): TradeSignal {
    if (!this.isWarmUpProcessed) {
      return { signal: 'hold', reason: 'Warm-up in progress', confidence: 0 };
    }

    if (!this.isInTradingSession(incoming.timestamp)) {
      return { signal: 'hold', reason: 'Out of session', confidence: 0 };
    }

    const prevClose3m = this.bars3min.length ? this.bars3min[this.bars3min.length - 1].close : NaN;
    const vol = Number.isFinite(incoming.volume as any) ? Number(incoming.volume) : 0;
    const signedVol =
      Number.isFinite(prevClose3m) && Number.isFinite(incoming.close)
        ? (incoming.close > prevClose3m ? vol : incoming.close < prevClose3m ? -vol : 0)
        : 0;

    const bar: BarData = {
      ...incoming,
      delta: (typeof incoming.delta === 'number' && Number.isFinite(incoming.delta))
        ? Math.trunc(incoming.delta)
        : Math.trunc(signedVol),
    };

    this.bars3min.push(bar);
    if (this.bars3min.length > 2000) this.bars3min.shift();
    this.updateHigherTimeframeBars(bar);

    const atr = this.calculateATR();
    const trend = this.determineTrend();
    const { brokeUpCloseTol, brokeDownCloseTol } = this.checkBreakoutCloseTol();
    const { passLong, passShort } = this.checkLtfEmaFilter();

    marketState.atr = Number.isFinite(atr) ? atr : 0;
    marketState.higherTimeframeTrend = trend;
    marketState.deltaCumulative = (marketState.deltaCumulative ?? 0) + (bar.delta ?? 0);

    const exitSignal = this.checkExitConditions(bar, marketState);
    if (exitSignal) return exitSignal;

    return this.generateSignal(bar, marketState, { brokeUpCloseTol, brokeDownCloseTol, passLong, passShort });
  }

  private updateHigherTimeframeBars(bar: BarData): void {
    const htfMin = Math.max(1, Number((this.config as any).higherTimeframe ?? 15));
    const stepMs = htfMin * 60 * 1000;
    const tsMs = new Date(bar.timestamp).getTime();
    const bucketStartMs = Math.floor(tsMs / stepMs) * stepMs;
    const last = this.bars15min[this.bars15min.length - 1];

    if (!last || this.lastHTFBucketStartMs === null || bucketStartMs > this.lastHTFBucketStartMs) {
      this.bars15min.push({ ...bar });
      this.lastHTFBucketStartMs = bucketStartMs;
      if (this.bars15min.length > 1000) this.bars15min.shift();
      return;
    }

    last.high = Math.max(last.high, bar.high);
    last.low = Math.min(last.low, bar.low);
    last.close = bar.close;
    last.volume = (last.volume ?? 0) + (bar.volume ?? 0);
    if (typeof bar.delta === 'number') last.delta = (last.delta ?? 0) + bar.delta;
  }

  private calculateATR(): number {
    const period = 14;
    if (this.bars3min.length < period + 1) return NaN;
    const validTail: Array<{ open: number; high: number; low: number; close: number }> = [];
    for (let i = this.bars3min.length - 1; i >= 0 && validTail.length < period + 1; i--) {
      const b = this.bars3min[i];
      if ([b.open, b.high, b.low, b.close].every(v => Number.isFinite(v))) {
        validTail.push({ open: b.open, high: b.high, low: b.low, close: b.close });
      }
    }
    if (validTail.length < period + 1) return NaN;
    validTail.reverse();
    const tr: number[] = [];
    for (let i = 1; i < validTail.length; i++) {
      const h = validTail[i].high;
      const l = validTail[i].low;
      const prevC = validTail[i - 1].close;
      tr.push(Math.max(h - l, Math.abs(h - prevC), Math.abs(l - prevC)));
    }
    if (tr.length < period) return NaN;
    let atr = tr.slice(0, period).reduce((a, b) => a + b, 0) / period;
    for (let i = period; i < tr.length; i++) {
      atr = (atr * (period - 1) + tr[i]) / period;
    }
    return atr;
  }

  private checkLtfEmaFilter(): { passLong: boolean; passShort: boolean; lastClose: number; lastEma: number } {
    if (!this.config.useEmaFilter) {
      const lastClose = this.bars3min.length ? this.bars3min[this.bars3min.length - 1].close : NaN;
      return { passLong: true, passShort: true, lastClose, lastEma: NaN };
    }
    const L = Math.max(1, this.config.emaLength ?? 21);
    const closes = this.bars3min.map(b => b.close);
    if (closes.length < L) {
      return { passLong: false, passShort: false, lastClose: NaN, lastEma: NaN };
    }
    const emaSeries = this.technical.calculateEMA(closes, L);
    const lastClose = closes[closes.length - 1];
    const lastEma = emaSeries[emaSeries.length - 1];
    return { passLong: lastClose > lastEma, passShort: lastClose < lastEma, lastClose, lastEma };
  }

  private determineTrend(): 'bullish' | 'bearish' | 'neutral' {
    if (this.bars15min.length < 2) return 'neutral';
    const L = Math.max(1, this.config.htfEMALength ?? 9);
    const useForming = this.config.htfUseForming === true;
    const lastIdx = useForming ? this.bars15min.length - 1 : this.bars15min.length - 2;
    if (lastIdx < 0) return 'neutral';
    const closes = this.bars15min.slice(0, lastIdx + 1).map(b => b.close);
    if (closes.length < L) return 'neutral';
    const emaSeries = this.technical.calculateEMA(closes, L);
    const px = closes[closes.length - 1];
    const ema = emaSeries[emaSeries.length - 1];
    return px > ema ? 'bullish' : px < ema ? 'bearish' : 'neutral';
  }

  // Breakout tolerance: 0.5% buffer to match Pine's forgiving detection
  private checkBreakoutCloseTol(): { brokeUpCloseTol: boolean; brokeDownCloseTol: boolean } {
    const n = Math.max(1, this.config.breakoutLookbackBars ?? 20);
    if (this.bars3min.length < n + 1) return { brokeUpCloseTol: false, brokeDownCloseTol: false };
    const recent = this.bars3min.slice(-n - 1, -1);
    const lastClose = this.bars3min[this.bars3min.length - 1].close;
    const high = Math.max(...recent.map(b => b.high));
    const low = Math.min(...recent.map(b => b.low));
    // Apply 0.5% tolerance buffer
    return { 
      brokeUpCloseTol: lastClose > high * 0.995, 
      brokeDownCloseTol: lastClose < low * 1.005 
    };
  }

  // Breakout tolerance for forming bar: same 0.5% buffer
  private checkBreakoutCloseTolForming(forming: BarData) {
    const n = Math.max(1, this.config.breakoutLookbackBars ?? 20);
    if (this.bars3min.length < n) {
      return { brokeUpCloseTol: false, brokeDownCloseTol: false };
    }
    const window = this.bars3min.slice(-n);
    const hi = Math.max(...window.map(b => b.high));
    const lo = Math.min(...window.map(b => b.low));
    // Apply 0.5% tolerance buffer
    return { 
      brokeUpCloseTol: forming.close > hi * 0.995, 
      brokeDownCloseTol: forming.close < lo * 1.005 
    };
  }

  private checkExitConditions(bar: BarData, _marketState: MarketState): TradeSignal | null {
    if (!this.currentPosition) return null;
    const { entryTime, direction, stopLoss } = this.currentPosition;
    const minBars = Math.max(0, this.config.minBarsBeforeExit ?? 0);
    const barsSinceEntry = this.bars3min.filter(b => new Date(b.timestamp).getTime() > entryTime).length;
    if (barsSinceEntry < minBars) return null;
    if (direction === 'long' && bar.low <= stopLoss) {
      return { signal: 'sell', reason: `Hit stop (${stopLoss.toFixed(2)})`, confidence: 1.0 };
    }
    if (direction === 'short' && bar.high >= stopLoss) {
      return { signal: 'buy', reason: `Hit stop (${stopLoss.toFixed(2)})`, confidence: 1.0 };
    }
    return null;
  }

  private smaSignedDelta(n: number, endIndex: number): number {
    if (endIndex < 0) return NaN;
    const start = Math.max(0, endIndex - n + 1);
    if (endIndex - start + 1 < n) return NaN;
    let sum = 0;
    for (let i = start; i <= endIndex; i++) {
      const d = (this.bars3min[i].delta ?? (this.bars3min[i].close - (this.bars3min[i - 1]?.close ?? this.bars3min[i].open)));
      sum += Number(d) || 0;
    }
    return sum / n;
  }

  private generateSignal(
    bar: BarData,
    marketState: MarketState,
    gates: { brokeUpCloseTol: boolean; brokeDownCloseTol: boolean; passLong: boolean; passShort: boolean }
  ): TradeSignal {
    const { brokeUpCloseTol, brokeDownCloseTol, passLong, passShort } = gates;
    console.debug(`[signal][bar] Δ=${bar.delta} HTF=${marketState.higherTimeframeTrend} breakUp=${brokeUpCloseTol} breakDn=${brokeDownCloseTol} emaL=${passLong} emaS=${passShort} ATR=${marketState.atr?.toFixed(2)}`);
    
    if (this.lastEntryBarTimestamp === bar.timestamp) {
      return { signal: 'hold', reason: 'Same bar re-entry blocked', confidence: 0 };
    }

    const atr = marketState.atr;
    const atrThreshold = this.config.minAtrToTrade ?? 0;
    if (!(Number.isFinite(atr) && atr > atrThreshold)) {
      return { signal: 'hold', reason: `ATR ${atr.toFixed(2)} ≤ ${atrThreshold}`, confidence: 0 };
    }

    const spike = this.config.deltaSpikeThreshold ?? 450;
    const delta = bar.delta ?? 0;
    const len = Math.max(1, this.config.deltaSMALength ?? 20);
    const deltaSMA = this.smaSignedDelta(len, this.bars3min.length - 1);
    if (!Number.isFinite(deltaSMA)) {
      return { signal: 'hold', reason: 'Delta SMA not ready', confidence: 0 };
    }

    // Fade check for bar-close path
    if (this.bars3min.length >= 2) {
      const prevDelta = this.bars3min[this.bars3min.length - 2].delta ?? 0;
      const peakAbs = Math.max(Math.abs(prevDelta), Math.abs(delta));
      const currAbs = Math.abs(delta);
      const fadeOk = peakAbs === 0 || currAbs >= peakAbs * (this.config.deltaFadeRatio ?? 0.7);
      if (!fadeOk) {
        return { signal: 'hold', reason: `Bar-close fade: ${currAbs} < 70% of peak ${peakAbs}`, confidence: 0 };
      }
    }

    const surgeMult = this.config.deltaSurgeMultiplier ?? 1.8;
    const longThreshold = deltaSMA * surgeMult;
    const shortThreshold = deltaSMA * -surgeMult;

    const passDeltaLong = delta > spike && delta > longThreshold;
    const passDeltaShort = delta < -spike && delta < shortThreshold;

    const htf = marketState.higherTimeframeTrend;

    if (passDeltaLong && htf === 'bullish' && brokeUpCloseTol) {
      if (this.config.useEmaFilter && !passLong) {
        return { signal: 'hold', reason: 'LTF EMA long filter not passed', confidence: 0 };
      }
      this.lastEntryBarTimestamp = bar.timestamp;
      return { signal: 'buy', reason: `Δ=${delta} > spike & SMA×mult, bullish HTF`, confidence: 0.9 };
    }
    if (passDeltaShort && htf === 'bearish' && brokeDownCloseTol) {
      if (this.config.useEmaFilter && !passShort) {
        return { signal: 'hold', reason: 'LTF EMA short filter not passed', confidence: 0 };
      }
      this.lastEntryBarTimestamp = bar.timestamp;
      return { signal: 'sell', reason: `Δ=${delta} < -spike & SMA×(-mult), bearish HTF`, confidence: 0.9 };
    }

    return { signal: 'hold', reason: 'No signal', confidence: 0 };
  }

  // === INTRA-BAR ===
  public evaluateFormingBar(
    formingBar: BarData,
    marketState: MarketState,
    accumulationTimeMs: number
  ): TradeSignal {
    if (!this.isWarmUpProcessed) return { signal: 'hold', reason: 'Warm-up incomplete', confidence: 0 };
    if (this.hasPosition()) return { signal: 'hold', reason: 'Already in position', confidence: 0 };

    if (!this.isInTradingSession(formingBar.timestamp)) {
      return { signal: 'hold', reason: 'Out of session (intra-bar)', confidence: 0 };
    }

    const minAccumMs = this.config.intraBarMinAccumulationMs ?? 3000;
    if (accumulationTimeMs < minAccumMs) {
      return { signal: 'hold', reason: `Accum < ${minAccumMs}ms`, confidence: 0 };
    }

    const nowMs = Date.now();
    const confirmWindowMs = this.config.intraBarConfirmationWindowMs ?? 500;
    const recentHistory = this.intraBarDeltaHistory.filter(e => nowMs - e.timestamp <= confirmWindowMs);

    const required = this.config.intraBarConfirmationChecks ?? 3;
    if (recentHistory.length < required) {
      return { signal: 'hold', reason: `Need ${required} confirms`, confidence: 0 };
    }

    if (nowMs - this.lastIntraBarSignalTime < 2000) {
      return { signal: 'hold', reason: 'Cooldown', confidence: 0 };
    }

    const atr = this.calculateATR();
    const trend = this.determineTrend();
    const { brokeUpCloseTol, brokeDownCloseTol } = this.checkBreakoutCloseTolForming(formingBar);

    const { lastEma } = this.checkLtfEmaFilter();
    if (this.config.useEmaFilter && !Number.isFinite(lastEma)) {
      return { signal: 'hold', reason: 'EMA not ready', confidence: 0 };
    }
    const passLong = !this.config.useEmaFilter || formingBar.close > lastEma;
    const passShort = !this.config.useEmaFilter || formingBar.close < lastEma;

    marketState.atr = Number.isFinite(atr) ? atr : 0;
    marketState.higherTimeframeTrend = trend;

    const signal = this.generateSignalForFormingBar(formingBar, marketState, { brokeUpCloseTol, brokeDownCloseTol, passLong, passShort });

    if (signal.signal !== 'hold') {
      this.lastIntraBarSignalTime = nowMs;
    }

    return signal;
  }

  private generateSignalForFormingBar(
    formingBar: BarData,
    marketState: MarketState,
    gates: { brokeUpCloseTol: boolean; brokeDownCloseTol: boolean; passLong: boolean; passShort: boolean }
  ): TradeSignal {
    const { brokeUpCloseTol, brokeDownCloseTol, passLong, passShort } = gates;
    console.debug(`[signal][intra] Δ=${formingBar.delta} HTF=${marketState.higherTimeframeTrend} breakUp=${brokeUpCloseTol} breakDn=${brokeDownCloseTol} emaL=${passLong} emaS=${passShort} ATR=${marketState.atr?.toFixed(2)}`);
    const atr = marketState.atr;
    const atrThreshold = this.config.minAtrToTrade ?? 0;
    if (!(Number.isFinite(atr) && atr > atrThreshold)) {
      return { signal: 'hold', reason: 'ATR gate failed', confidence: 0 };
    }

    const spike = this.config.deltaSpikeThreshold ?? 450;
    const delta = formingBar.delta ?? 0;
    const len = Math.max(1, this.config.deltaSMALength ?? 20);
    const deltaSMA = this.smaSignedDelta(len, this.bars3min.length - 1);
    if (!Number.isFinite(deltaSMA)) {
      return { signal: 'hold', reason: 'Delta SMA not ready', confidence: 0 };
    }

    const surgeMult = this.config.deltaSurgeMultiplier ?? 1.8;
    const longThreshold = deltaSMA * surgeMult;
    const shortThreshold = deltaSMA * -surgeMult;

    // Restored fade check for intra-bar - includes current delta in peakAbs
    const peakAbs = Math.max(...this.intraBarDeltaHistory.map(e => Math.abs(e.delta)), Math.abs(delta), 0);
    const currAbs = Math.abs(delta);
    const fadeOk = peakAbs === 0 || currAbs >= peakAbs * (this.config.deltaFadeRatio ?? 0.7);

    if (!fadeOk) {
      return { signal: 'hold', reason: `Intra fade: ${currAbs} < 70% of peak ${peakAbs}`, confidence: 0 };
    }

    const passDeltaLong = delta > spike && delta > longThreshold && fadeOk;
    const passDeltaShort = delta < -spike && delta < shortThreshold && fadeOk;

    const htf = marketState.higherTimeframeTrend;

    if (passDeltaLong && htf === 'bullish' && brokeUpCloseTol) {
      if (this.config.useEmaFilter && !passLong) return { signal: 'hold', reason: 'EMA filter', confidence: 0 };
      return { signal: 'buy', reason: `[INTRA] Δ=${delta} (fadeOK)`, confidence: 0.85 };
    }
    if (passDeltaShort && htf === 'bearish' && brokeDownCloseTol) {
      if (this.config.useEmaFilter && !passShort) return { signal: 'hold', reason: 'EMA filter', confidence: 0 };
      return { signal: 'sell', reason: `[INTRA] Δ=${delta} (fadeOK)`, confidence: 0.85 };
    }

    return { signal: 'hold', reason: 'No intra signal', confidence: 0 };
  }

  public resetIntraBarTracking(): void {
    this.intraBarDeltaHistory = [];
  }

  public clearCooldowns(): void {
    this.lastIntraBarSignalTime = 0;
    this.lastEntryBarTimestamp = null;
  }

  public clearPosition(): void {
    this.currentPosition = null;
    this.trailingStopLevel = 0;
    this.trailArmed = false;
  }

  public hasPosition(): boolean {
    return !!this.currentPosition;
  }

  public getPositionDirection(): 'long' | 'short' | null {
    return this.currentPosition?.direction ?? null;
  }

  public setPosition(entryPrice: number, direction: 'long' | 'short', atrForTrail?: number): void {
    const liveAtr = (typeof atrForTrail === 'number' && atrForTrail > 0) ? atrForTrail : this.atrAtSignal;
    
    // Stop uses capped ATR (limits max loss)
    const atrSeedForStop = this.config.useAtrCap
      ? Math.min(liveAtr, Number(this.config.atrCap ?? 16))
      : liveAtr;
    
    // Trail uses uncapped ATR (lets winners breathe)
    const atrSeedForTrail = liveAtr;

    const slDist = atrSeedForStop * (this.config.atrStopLossMultiplier ?? 0.75);
    const stopLoss = direction === 'long' ? entryPrice - slDist : entryPrice + slDist;

    this.currentPosition = { entryPrice, entryTime: Date.now(), direction, stopLoss, atrSeedForStop, atrSeedForTrail };
    this.trailingStopLevel = stopLoss;
    this.trailArmed = false;
    this.noTrailBeforeMs = Date.now() + (this.config.tickExitGraceMs ?? 0);
  }

  public captureAtrAtSignal(atr: number): void {
    this.atrAtSignal = atr;
  }

  public onTickForProtectiveStops(lastPrice: number, _atrNow: number): 'none' | 'hitStop' | 'hitTrail' {
    if (!this.currentPosition || !Number.isFinite(lastPrice)) return 'none';
    const { direction: dir, entryPrice, stopLoss, atrSeedForTrail } = this.currentPosition;

    if (dir === 'long' && lastPrice <= stopLoss) return 'hitStop';
    if (dir === 'short' && lastPrice >= stopLoss) return 'hitStop';

    if (Date.now() < this.noTrailBeforeMs) return 'none';
    if (!Number.isFinite(atrSeedForTrail) || atrSeedForTrail <= 0) return 'none';

    const act = atrSeedForTrail * (this.config.trailActivationATR ?? 0.125);
    const off = atrSeedForTrail * (this.config.trailOffsetATR ?? 0.125);

    if (dir === 'long') {
      if (!this.trailArmed && (lastPrice - entryPrice) >= act) {
        this.trailArmed = true;
        this.trailingStopLevel = Math.max(stopLoss, lastPrice - off);
      }
      if (this.trailArmed) {
        const candidate = Math.max(stopLoss, lastPrice - off);
        if (candidate > this.trailingStopLevel) this.trailingStopLevel = candidate;
        if (lastPrice <= this.trailingStopLevel) return 'hitTrail';
      }
    } else {
      if (!this.trailArmed && (entryPrice - lastPrice) >= act) {
        this.trailArmed = true;
        this.trailingStopLevel = Math.min(stopLoss, lastPrice + off);
      }
      if (this.trailArmed) {
        const candidate = Math.min(stopLoss, lastPrice + off);
        if (candidate < this.trailingStopLevel) this.trailingStopLevel = candidate;
        if (lastPrice >= this.trailingStopLevel) return 'hitTrail';
      }
    }
    return 'none';
  }

  public calculatePositionSize(currentPrice: number, atr: number, accountBalance: number): number {
    void currentPrice;
    const riskAmount = accountBalance * 0.01;
    const riskPerContract = atr * (this.config.atrStopLossMultiplier ?? 0.75);
    if (!Number.isFinite(riskPerContract) || riskPerContract <= 0) return 1;
    const size = Math.floor(riskAmount / riskPerContract);
    return Math.min(Math.max(1, size), this.config.contractQuantity ?? 1);
  }

  public getWarmUpStatus() {
    return { isComplete: this.isWarmUpProcessed, bars3min: this.bars3min.length, bars15min: this.bars15min.length };
  }
}