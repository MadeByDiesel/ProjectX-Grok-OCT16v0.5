// src/strategies/mnq-delta-trend/trader.ts
import { ProjectXClient } from '../../services/projectx-client';
import { MNQDeltaTrendCalculator } from './calculator';
import { StrategyConfig } from './types';
import { GatewayQuote, BarData } from '../../types';
import { execFile, ExecFileException, ExecFileOptionsWithStringEncoding } from 'child_process';

export class MNQDeltaTrendTrader {
  private client: ProjectXClient;
  private calculator: MNQDeltaTrendCalculator;
  private config: StrategyConfig;

  private contractId: string;
  private symbol: string;

  // Tick → bar accumulators
  private lastPriceByContract = new Map<string, number>();
  private lastCumVolByContract = new Map<string, number>();
  private signedVolInBarByContract = new Map<string, number>();
  private volInBarByContract = new Map<string, number>();
  private prevClosedBarClose: number | null = null;

  // Open 3m bar state
  private barOpenPx: number | null = null;
  private barHighPx: number | null = null;
  private barLowPx: number | null = null;
  private barStartMs: number | null = null;
  private readonly barStepMs = 3 * 60 * 1000;

  // Live forming bar tracking for intra-bar detection
  private liveBarOpen: number | null = null;
  private liveBarHigh: number | null = null;
  private liveBarLow: number | null = null;
  private liveBarStartMs: number | null = null;
  private lastIntraBarCheckMs = 0;

  // Per-bar entry tracking and async lock
  private enteredBarStartMs: number | null = null;
  private isEnteringPosition = false;

  private running = false;
  private heartbeat: NodeJS.Timeout | null = null;
  private isFlattening = false;

  // Single-writer reconcile lock for entry/exit/bar-close
  private reconciling = false;

  // Minimal market state (ATR/HTF filled by calculator on bar-close; ATR used here only for snapshot)
  private marketState = {
    atr: 0,
    higherTimeframeTrend: 'neutral' as 'bullish' | 'bearish' | 'neutral',
    deltaCumulative: 0
  };

  private marketDataHandler = (q: GatewayQuote & { contractId: string }) => this.onQuote(q);

  /** Post trade events to local NT8 webhook listener (optional) */
  private async postWebhook(action: 'BUY' | 'SELL' | 'FLAT', qty?: number): Promise<void> {
    if (!this.config?.sendWebhook) return;
    const base = this.config.webhookUrl || '';
    if (!base) return;

    const secret = (this as any).config?.webhookSecret;
    const url = (!base.includes('?') && secret) ? `${base}?secret=${secret}` : base;

    const payload: Record<string, any> = { symbol: 'MNQ', action };
    if (action !== 'FLAT') payload.qty = Math.max(1, Number(qty ?? 1));

    const body = JSON.stringify(payload);
    const localIf = (this as any).config?.webhookInterface || '192.168.4.50';

    const args: string[] = [
      '--interface', localIf,
      '--fail',
      '-sS',
      '-X', 'POST',
      '-H', 'Content-Type: application/json',
      '--max-time', '3',
      '--data-binary', body,
      url
    ];

    const opts: ExecFileOptionsWithStringEncoding = { timeout: 4000, encoding: 'utf8' };

    await new Promise<void>((resolve) => {
      execFile(
        '/usr/bin/curl',
        args,
        opts,
        (error: ExecFileException | null, stdout: string, stderr: string) => {
          if (error) {
            console.error('[webhook] curl error', error.message, stderr || '');
            return resolve();
          }
          if (stdout?.trim()) console.info('[webhook] sent', payload, 'resp=', stdout.trim());
          else console.info('[webhook] sent', payload);
          resolve();
        }
      );
    });
  }

  constructor(opts: {
    client: ProjectXClient;
    calculator: MNQDeltaTrendCalculator;
    config: StrategyConfig;
    contractId: string;
    symbol: string;
  }) {
    this.client = opts.client;
    this.calculator = opts.calculator;
    this.config = opts.config;
    this.contractId = opts.contractId;
    this.symbol = opts.symbol;
  }

  public async start(): Promise<void> {
    this.running = true;

    await this.client.connectWebSocket();
    await this.client.getSignalRService().subscribeToMarketData(this.contractId);

    this.client.onMarketData(this.marketDataHandler);

    if (this.heartbeat) clearInterval(this.heartbeat);
    this.heartbeat = setInterval(() => {
      if (!this.running) return;
      this.maybeCloseBarByClock();
    }, 1000);
  }

  public async stop(): Promise<void> {
    this.running = false;
    if (this.heartbeat) {
      clearInterval(this.heartbeat);
      this.heartbeat = null;
    }
    try { await this.client.disconnectWebSocket(); } catch {}
    console.info('[MNQDeltaTrend][Trader] stopped');
  }

  private onQuote(q: GatewayQuote & { contractId: string }) {
    if (!this.running) return;
    if (q.contractId !== this.contractId) return;

    const contractId = q.contractId;
    const px = q.lastPrice;
    if (!Number.isFinite(px)) return;

    const nowMs = Date.now();

    // === 3-minute bar bucketing (clock-based) ===
    const bucketStart = Math.floor(nowMs / this.barStepMs) * this.barStepMs;

    if (this.barStartMs === null) {
      // First tick ever
      this.barStartMs = bucketStart;
      this.barOpenPx = px;
      this.barHighPx = px;
      this.barLowPx = px;
      this.liveBarOpen = px;
      this.liveBarHigh = px;
      this.liveBarLow = px;
      this.liveBarStartMs = nowMs;
      this.volInBarByContract.set(contractId, 0);
      this.signedVolInBarByContract.set(contractId, 0);
    } else if (bucketStart > this.barStartMs) {
      // New bucket → close prior, open new
      this.closeBarAndProcess();

      this.barStartMs = bucketStart;
      this.barOpenPx = px;
      this.barHighPx = px;
      this.barLowPx = px;
      this.liveBarOpen = px;
      this.liveBarHigh = px;
      this.liveBarLow = px;
      this.liveBarStartMs = nowMs;
      this.lastIntraBarCheckMs = 0;

      this.enteredBarStartMs = null;
      this.calculator.resetIntraBarTracking();
      this.volInBarByContract.set(contractId, 0);
      this.signedVolInBarByContract.set(contractId, 0);
    } else {
      // Update current bar extremes
      this.barHighPx = Math.max(this.barHighPx!, px);
      this.barLowPx = Math.min(this.barLowPx!, px);
      this.liveBarHigh = Math.max(this.liveBarHigh!, px);
      this.liveBarLow = Math.min(this.liveBarLow!, px);
    }

    // === Per-tick delta & volume accumulation (cumulative → delta) ===
    const prevPx = this.lastPriceByContract.get(contractId);
    const prevCum = this.lastCumVolByContract.get(contractId);
    const cumVol = (q as any).volume ?? 0; // GatewayQuote 'volume' treated as cumulative

    let dVol = 0;
    if (typeof prevCum === 'number' && cumVol >= prevCum) dVol = cumVol - prevCum;

// Accumulate volume
    this.volInBarByContract.set(contractId, (this.volInBarByContract.get(contractId) ?? 0) + (Number.isFinite(dVol) ? dVol : 0));

    // Per-tick signed delta for exhaustion tracking
    const signed = typeof prevPx === 'number'
      ? (px > prevPx ? dVol : px < prevPx ? -dVol : 0)
      : 0;

    // Pine parity: recalculate bar delta vs previous closed bar (not tick-to-tick accumulation)
    const barVol = this.volInBarByContract.get(contractId) ?? 0;
    let barDelta = 0;
    if (this.prevClosedBarClose !== null) {
      if (px > this.prevClosedBarClose) {
        barDelta = barVol;
      } else if (px < this.prevClosedBarClose) {
        barDelta = -barVol;
      }
    }
    this.signedVolInBarByContract.set(contractId, barDelta);
  
    // Push per-tick signed delta into calculator's intra-bar window
    this.calculator.pushIntraBarDelta(signed, nowMs);

    // Now update last refs
    this.lastPriceByContract.set(contractId, px);
    this.lastCumVolByContract.set(contractId, cumVol);

    // === Tick-level protective exits (hard stop / trail) ===
    if (this.calculator.hasPosition() && !this.isFlattening) {
      if (this.reconciling) return;
      const hit = this.calculator.onTickForProtectiveStops(px, this.marketState.atr ?? 0);
      if (hit === 'hitStop' || hit === 'hitTrail') {
        const dir = this.calculator.getPositionDirection();
        console.info(`[MNQDeltaTrend][EXIT] ${hit} (tick) px=${px} dir=${dir}`);
        this.reconciling = true;
        this.isFlattening = true;
        this.client.closePosition(this.contractId)
          .then(() => {
            console.info('[MNQDeltaTrend][EXIT] flattened');
            this.calculator.clearPosition();
            this.calculator.clearCooldowns();
            this.isFlattening = false;
            this.reconciling = false;
            if (this.config.sendWebhook) void this.postWebhook('FLAT');
          })
          .catch(err => {
            console.error('[MNQDeltaTrend][EXIT] flatten failed:', err);
            this.isFlattening = false;
            this.reconciling = false;
          });
      }
    }

    // === Intra-bar signal check ===
    if (this.config.useIntraBarDetection && !this.calculator.hasPosition() && !this.isFlattening) {
      const checkIntervalMs = this.config.intraBarCheckIntervalMs ?? 100;
      if ((nowMs - this.lastIntraBarCheckMs) >= checkIntervalMs) {
        this.lastIntraBarCheckMs = nowMs;
        this.checkIntraBarSignal(px, nowMs);
      }
    }
  }

  private maybeCloseBarByClock(): void {
    if (!this.running) return;
    if (this.barStartMs === null) return;

    const nowMs = Date.now();
    const bucketStart = Math.floor(nowMs / this.barStepMs) * this.barStepMs;
    if (bucketStart > this.barStartMs) {
      const lastPx = this.lastPriceByContract.get(this.contractId);
      if (!Number.isFinite(lastPx)) return;

      this.closeBarAndProcess();

      this.barStartMs = bucketStart;
      this.barOpenPx = lastPx!;
      this.barHighPx = lastPx!;
      this.barLowPx = lastPx!;
      this.liveBarOpen = lastPx!;
      this.liveBarHigh = lastPx!;
      this.liveBarLow = lastPx!;
      this.liveBarStartMs = nowMs;
      this.lastIntraBarCheckMs = 0;
      this.enteredBarStartMs = null;
      this.calculator.resetIntraBarTracking();
      this.volInBarByContract.set(this.contractId, 0);
      this.signedVolInBarByContract.set(this.contractId, 0);
    }
  }

  private checkIntraBarSignal(currentPrice: number, nowMs: number): void {
    if (this.barStartMs === null) return;

    const formingBar: BarData = {
      timestamp: new Date(this.barStartMs + this.barStepMs - 1).toISOString(), // end-of-bucket timestamp for gate
      open: this.liveBarOpen!,
      high: this.liveBarHigh!,
      low: this.liveBarLow!,
      close: currentPrice,
      volume: this.volInBarByContract.get(this.contractId) ?? 0,
      delta: this.signedVolInBarByContract.get(this.contractId) ?? 0,
    };

    const accumulationMs = this.liveBarStartMs ? (nowMs - this.liveBarStartMs) : 0;
    const signal = this.calculator.evaluateFormingBar(formingBar, this.marketState, accumulationMs);

    if (signal.signal !== 'hold') {
      // Route through unified handler (applies race guard + ATR snapshot + order)
      void this.executeIntraBarSignal(signal, formingBar);
    }
  }

  private async executeIntraBarSignal(
    signal: { signal: 'buy' | 'sell' | 'hold'; reason: string; confidence: number },
    bar: BarData
  ) {
    await this.handleSignal(signal, bar);
  }

  private closeBarAndProcess(): void {
    if (this.barStartMs === null || this.barOpenPx === null || this.barHighPx === null || this.barLowPx === null) return;

    const closePx = this.lastPriceByContract.get(this.contractId);
    if (!Number.isFinite(closePx)) return;

    const volume = Math.max(0, Math.floor(this.volInBarByContract.get(this.contractId) ?? 0));
    const signed = Math.trunc(this.signedVolInBarByContract.get(this.contractId) ?? 0);

    const barEndIso = new Date(this.barStartMs + this.barStepMs - 1).toISOString();

    const closedBar: BarData = {
      timestamp: barEndIso,
      open: this.barOpenPx,
      high: this.barHighPx,
      low: this.barLowPx,
      close: closePx!,
      volume: volume,
      delta: signed,
    };

    // Store for next bar's delta calculation
    this.prevClosedBarClose = closePx!;

    // Reset accumulators for next bar
    this.volInBarByContract.set(this.contractId, 0);
    this.signedVolInBarByContract.set(this.contractId, 0);

    // Always update calculator state on every bar close
    const signal = this.calculator.processNewBar(closedBar as any, this.marketState as any);

    // Only ACT on bar-close signals when intrabar is OFF (and not reconciling)
    if (!this.config.useIntraBarDetection && !this.reconciling) {
      void this.handleSignal(signal, closedBar);
    } else {
      console.debug('[MNQDeltaTrend][barClose] state updated; orders suppressed (intra-bar ON or reconciling)');
    }
    
    console.debug(
      `[MNQDeltaTrend][barClose] t=${closedBar.timestamp} O:${closedBar.open} H:${closedBar.high} L:${closedBar.low} C:${closedBar.close} Δ:${closedBar.delta} V:${closedBar.volume}`
    );

    this.barOpenPx = closePx!;
    this.barHighPx = closePx!;
    this.barLowPx = closePx!;
  }

  private async handleSignal(
    signal: { signal: 'buy' | 'sell' | 'hold' | 'exit'; reason: string; confidence: number },
    bar: BarData
  ) {
    // Ignore calculator’s optional 'exit' (we exit via tick-level SL/trail)
    if (signal.signal === 'exit') return;

    if (signal.signal === 'hold') {
      console.debug('[MNQDeltaTrend][order] HOLD:', signal.reason);
      return;
    }

    if (this.calculator.hasPosition()) {
      console.debug('[MNQDeltaTrend][order] skipped: already in position');
      return;
    }
    if (this.isEnteringPosition || this.reconciling) {
      console.debug('[MNQDeltaTrend][order] skipped: entry in flight / reconciling');
      return;
    }
    if (this.enteredBarStartMs === this.barStartMs) {
      console.debug('[MNQDeltaTrend][order] skipped: already entered this bar');
      return;
    }

    const minAtr = Math.max(0, this.config.minAtrToTrade ?? 0);
    const atrNow = this.marketState.atr ?? 0;
    if (!Number.isFinite(atrNow) || atrNow < minAtr) {
      console.debug(`[MNQDeltaTrend][order] blocked: ATR gate failed (atr=${atrNow}, thresh=${minAtr})`);
      return;
    }

    // --- CRITICAL RACE GUARD: claim bar BEFORE awaits to block the other path ---
    const barId = this.barStartMs!;
    this.enteredBarStartMs = barId;

    const direction = signal.signal === 'buy' ? 'long' : 'short';
    const atrSnapshot = Math.min(atrNow, this.config.atrCap ?? 16); // enforce atrCap at snapshot

    this.isEnteringPosition = true;
    this.reconciling = true;

    try {
      // Freeze ATR at signal for SL/trail math
      this.calculator.captureAtrAtSignal(atrSnapshot);

      const acctBal = await this.client.getEquity();
      const qty = Math.max(1, this.calculator.calculatePositionSize(bar.close, atrSnapshot, acctBal));

      console.info(`[MNQDeltaTrend][order] ${signal.signal.toUpperCase()} qty=${qty} reason="${signal.reason}"`);

      await this.client.createOrder({
        contractId: this.contractId,
        type: 2,
        side: signal.signal === 'buy' ? 0 : 1,
        size: qty,
      });

      try {
        (this.calculator as any).setPosition?.(bar.close, direction, atrSnapshot);
      } catch (err) {
        console.warn('[MNQDeltaTrend] setPosition error:', err);
      }

      this.calculator.resetIntraBarTracking();

      if (this.config.sendWebhook) void this.postWebhook(signal.signal === 'buy' ? 'BUY' : 'SELL', qty);

    } catch (err) {
      console.error('[MNQDeltaTrend][order] placement failed:', err);
      // Roll back claim if order failed so other path can retry on this bar
      this.enteredBarStartMs = (this.barStartMs === barId) ? null : this.enteredBarStartMs;
    } finally {
      this.isEnteringPosition = false;
      this.reconciling = false;
    }
  }
}