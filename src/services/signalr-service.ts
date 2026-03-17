// src/services/signalr-service.ts
// Base: Oct16 original (clean GatewayQuote spread, GatewayTrade batch iteration)
// Added: staleness heartbeat, disconnectForReconnect, exponential backoff, tokenRefreshFn
import * as signalR from '@microsoft/signalr';
import { Logger } from '../utils/logger';
import {
  GatewayUserAccount,
  GatewayUserOrder,
  GatewayUserPosition,
  GatewayUserTrade,
  GatewayQuote,
  GatewayTrade,
  GatewayDepth
} from '../types';

export class SignalRService {
  private userHubConnection: signalR.HubConnection | null = null;
  private marketHubConnection: signalR.HubConnection | null = null;
  private jwtToken: string | null = null;
  private selectedAccountId: number | null = null;
  private logger: Logger;
  private eventCallbacks: Map<string, Function[]> = new Map();
  private firstQuoteLogged = false;
  private subscribedContracts = new Set<string>();

  // Staleness detection
  private lastTickTime: number = 0;
  private heartbeatInterval: NodeJS.Timeout | null = null;
  private isReconnecting: boolean = false;
  private consecutiveReconnectFailures: number = 0;
  private nextReconnectAttemptMs: number = 0;

  // Token refresh callback — set by projectx-client
  private tokenRefreshFn: (() => Promise<string>) | null = null;

  constructor() {
    this.logger = new Logger('SignalRService');
  }

  async initialize(jwtToken: string, selectedAccountId: number): Promise<void> {
    this.jwtToken = jwtToken;
    this.selectedAccountId = selectedAccountId;

    await this.initializeUserHub();
    await this.initializeMarketHub();

    this.lastTickTime = Date.now();
    this.startHeartbeat();
  }

  updateToken(newToken: string): void {
    this.jwtToken = newToken;
    this.logger.info('JWT token updated for SignalR');
  }

  setTokenRefreshFn(fn: () => Promise<string>): void {
    this.tokenRefreshFn = fn;
  }

  // ========== Staleness Detection ==========
  private startHeartbeat(): void {
    this.stopHeartbeat();
    this.heartbeatInterval = setInterval(() => this.checkStaleness(), 30000);
    this.logger.info('Staleness heartbeat started');
  }

  private stopHeartbeat(): void {
    if (this.heartbeatInterval) {
      clearInterval(this.heartbeatInterval);
      this.heartbeatInterval = null;
    }
  }

  private isMarketHours(): boolean {
    const now = new Date();
    const day = now.getUTCDay();
    const hour = now.getUTCHours();
    if (day === 6) return false;
    if (day === 0 && hour < 23) return false;
    if (day === 5 && hour >= 22) return false;
    if (hour === 22) return false;
    return true;
  }

  private async checkStaleness(): Promise<void> {
    if (this.isReconnecting || !this.isMarketHours() || this.subscribedContracts.size === 0) return;

    const staleMs = Date.now() - this.lastTickTime;
    if (staleMs <= 60000) {
      if (this.consecutiveReconnectFailures > 0) {
        this.logger.info(`Feed restored after ${this.consecutiveReconnectFailures} failed reconnect(s)`);
        this.consecutiveReconnectFailures = 0;
        this.nextReconnectAttemptMs = 0;
      }
      return;
    }

    if (Date.now() < this.nextReconnectAttemptMs) return;

    this.logger.warn(`No market data for ${Math.round(staleMs / 1000)}s - reconnect attempt #${this.consecutiveReconnectFailures + 1}`);
    this.isReconnecting = true;
    try {
      await this.disconnectForReconnect();

      if (this.tokenRefreshFn) {
        try {
          const freshToken = await this.tokenRefreshFn();
          this.jwtToken = freshToken;
          this.logger.info('Token refreshed before reconnect');
        } catch (tokenErr) {
          this.logger.error('Token refresh failed, attempting reconnect with existing token:', tokenErr);
        }
      }

      if (this.jwtToken && this.selectedAccountId) {
        await this.initializeUserHub();
        await this.initializeMarketHub();
        await this.resubscribeAllContracts();
        this.lastTickTime = Date.now();
        this.consecutiveReconnectFailures = 0;
        this.nextReconnectAttemptMs = 0;
        this.logger.info('Staleness reconnect completed — feed restored');
      }
    } catch (err) {
      this.consecutiveReconnectFailures++;
      const backoffSec = Math.min(30 * Math.pow(2, this.consecutiveReconnectFailures - 1), 300);
      this.nextReconnectAttemptMs = Date.now() + backoffSec * 1000;
      this.logger.error(`Staleness reconnect failed (attempt ${this.consecutiveReconnectFailures}, next retry in ${backoffSec}s):`, err);
    } finally {
      this.isReconnecting = false;
    }
  }

  private async disconnectForReconnect(): Promise<void> {
    try {
      if (this.userHubConnection) await this.userHubConnection.stop();
    } catch (err) {
      this.logger.warn('Error stopping User Hub during reconnect:', err);
    }
    try {
      if (this.marketHubConnection) await this.marketHubConnection.stop();
    } catch (err) {
      this.logger.warn('Error stopping Market Hub during reconnect:', err);
    }
    this.logger.info('SignalR connections stopped for reconnect (heartbeat still active)');
  }

  // ========== User Hub ==========
  private async initializeUserHub(): Promise<void> {
    if (!this.jwtToken || !this.selectedAccountId) {
      throw new Error('JWT token or account ID not set');
    }

    const userHubUrl = `https://rtc.topstepx.com/hubs/user?access_token=${this.jwtToken}`;

    this.userHubConnection = new signalR.HubConnectionBuilder()
      .withUrl(userHubUrl, {
        skipNegotiation: true,
        transport: signalR.HttpTransportType.WebSockets,
        accessTokenFactory: () => this.jwtToken!,
        timeout: 10000
      })
      .withAutomaticReconnect()
      .build();

    this.setupUserHubHandlers();

    try {
      await this.userHubConnection.start();
      this.logger.info('User Hub connected successfully');
      await this.subscribeToUserHub();
    } catch (error) {
      this.logger.error('Failed to start User Hub connection:', error);
      throw error;
    }
  }

  private setupUserHubHandlers(): void {
    const conn = this.userHubConnection;
    if (!conn) return;

    conn.on('GatewayUserAccount', (data: GatewayUserAccount) => {
      this.emit('account_update', data);
    });

    conn.on('GatewayUserOrder', (data: GatewayUserOrder) => {
      this.emit('order_update', data);
    });

    conn.on('GatewayUserPosition', (data: GatewayUserPosition) => {
      this.emit('position_update', data);
    });

    conn.on('GatewayUserTrade', (data: GatewayUserTrade) => {
      this.emit('trade_update', data);
    });

    conn.onreconnected(() => {
      this.logger.info('User Hub reconnected');
      this.subscribeToUserHub().catch((err) =>
        this.logger.error('Failed to re-subscribe User Hub after reconnect:', err)
      );
    });
  }

  private async subscribeToUserHub(): Promise<void> {
    if (!this.userHubConnection || !this.selectedAccountId) return;

    try {
      await this.userHubConnection.invoke('SubscribeAccounts');
      await this.userHubConnection.invoke('SubscribeOrders', this.selectedAccountId);
      await this.userHubConnection.invoke('SubscribePositions', this.selectedAccountId);
      await this.userHubConnection.invoke('SubscribeTrades', this.selectedAccountId);
      this.logger.info('Subscribed to User Hub events');
    } catch (error) {
      this.logger.error('Failed to subscribe to User Hub:', error);
    }
  }

  // ========== Market Hub ==========
  private async initializeMarketHub(): Promise<void> {
    if (!this.jwtToken) {
      throw new Error('JWT token not set');
    }

    const marketHubUrl = `https://rtc.topstepx.com/hubs/market?access_token=${this.jwtToken}`;

    this.marketHubConnection = new signalR.HubConnectionBuilder()
      .withUrl(marketHubUrl, {
        skipNegotiation: true,
        transport: signalR.HttpTransportType.WebSockets,
        accessTokenFactory: () => this.jwtToken!,
        timeout: 10000
      })
      .withAutomaticReconnect()
      .build();

    this.setupMarketHubHandlers();

    try {
      await this.marketHubConnection.start();
      this.logger.info('Market Hub connected successfully');
    } catch (error) {
      this.logger.error('Failed to start Market Hub connection:', error);
      throw error;
    }
  }

  private setupMarketHubHandlers(): void {
    const conn = this.marketHubConnection;
    if (!conn) {
      this.logger.warn('Market Hub connection not available when setting handlers');
      return;
    }

    conn.on('GatewayQuote', (contractId: string, data: GatewayQuote) => {
      this.lastTickTime = Date.now();
      if (!this.firstQuoteLogged) {
        this.logger.info(
          `First GatewayQuote: contractId=${contractId}, symbol=${(data as any).symbol ?? 'N/A'}, lastPrice=${data.lastPrice}`
        );
        this.firstQuoteLogged = true;
      }
      this.emit('market_data', { contractId, ...data });
    });

    conn.on('GatewayTrade', (contractId: string, payload: any) => {
      this.lastTickTime = Date.now();
      const raw = typeof payload === 'string' ? JSON.parse(payload) : payload;
      const trades = Array.isArray(raw) ? raw : [raw];
      for (const trade of trades) {
        if (!trade || typeof trade.price !== 'number') continue;
        this.emit('market_trade', {
          contractId,
          price: trade.price,
          volume: trade.volume ?? 1,
          type: trade.type,
          timestamp: trade.timestamp,
        });
      }
    });

    conn.on('GatewayDepth', (contractId: string, data: GatewayDepth) => {
      this.emit('market_depth', { contractId, ...data });
    });

    conn.onreconnected(async () => {
      this.logger.info('Market Hub reconnected — re-subscribing existing contracts');
      await this.resubscribeAllContracts();
      this.lastTickTime = Date.now();

      // --- Restore open MNQ position after reconnect ---
      try {
        const { projectXClient, trader } = global as any; // both already initialized in server.ts
        if (projectXClient && trader) {
          const openPositions = await projectXClient.searchOpenPositions();
          const mnq = openPositions.find((p: any) => p.contractId?.includes('MNQ'));
          if (mnq) {
            const side = mnq.side === 1 ? 'short' : 'long';
            const avgPrice = mnq.avgPrice;
            const currentATR = trader.calculator.calculateATR();
            trader.calculator.setPosition(avgPrice, side, currentATR);
            this.logger.info('[reconnect] Restored open MNQ position', { side, avgPrice });
          } else {
            this.logger.info('[reconnect] No open MNQ positions to restore');
          }
        } else {
          this.logger.warn('[reconnect] Trader or ProjectX client not available in global scope');
        }
      } catch (err) {
        this.logger.error('[reconnect] Failed to restore open position', err);
      }
    });
  }

  // ========== Subscriptions ==========
  /**
   * Batch subscribe to quotes + trades for multiple contracts.
   * Tracks subscriptions for reconnect.
   */
  async subscribeToContracts(contractIds: string[]): Promise<void> {
    const conn = this.marketHubConnection;
    if (!conn || contractIds.length === 0) return;

    for (const id of contractIds) {
      try {
        await conn.invoke('SubscribeContractQuotes', id);
        await conn.invoke('SubscribeContractTrades', id);
        this.subscribedContracts.add(id);
        this.logger.info(`Subscribed to market data for contract: ${id}`);
      } catch (error) {
        this.logger.error(`Failed to subscribe to market data for ${id}:`, error);
      }
    }
  }

  /**
   * Backward-compatible single-contract subscribe. (Used by projectx-client)
   */
  async subscribeToMarketData(contractId: string): Promise<void> {
    return this.subscribeToContracts([contractId]);
  }

  async unsubscribeFromMarketData(contractId: string): Promise<void> {
    const conn = this.marketHubConnection;
    if (!conn) return;

    try {
      await conn.invoke('UnsubscribeContractQuotes', contractId);
      await conn.invoke('UnsubscribeContractTrades', contractId);
      this.subscribedContracts.delete(contractId);
      this.logger.info(`Unsubscribed from market data for contract: ${contractId}`);
    } catch (error) {
      this.logger.error('Failed to unsubscribe from market data:', error);
    }
  }

  private async resubscribeAllContracts(): Promise<void> {
    const conn = this.marketHubConnection;
    if (!conn || this.subscribedContracts.size === 0) return;

    for (const id of this.subscribedContracts) {
      try {
        await conn.invoke('SubscribeContractQuotes', id);
        await conn.invoke('SubscribeContractTrades', id);
        this.logger.info(`Re-subscribed contract after reconnect: ${id}`);
      } catch (err) {
        this.logger.error(`Failed to re-subscribe contract ${id} after reconnect`, err);
      }
    }
  }

  // ========== Events ==========
  on(event: string, callback: Function): void {
    if (!this.eventCallbacks.has(event)) {
      this.eventCallbacks.set(event, []);
    }
    this.eventCallbacks.get(event)!.push(callback);
  }

  private emit(event: string, data: any): void {
    const callbacks = this.eventCallbacks.get(event) || [];
    callbacks.forEach((callback) => {
      try {
        callback(data);
      } catch (err) {
        this.logger.error(`Error in '${event}' callback:`, err);
      }
    });
  }

  // ========== Lifecycle ==========
  async disconnect(): Promise<void> {
    this.stopHeartbeat();
    this.consecutiveReconnectFailures = 0;
    this.nextReconnectAttemptMs = 0;
    if (this.userHubConnection) {
      await this.userHubConnection.stop();
    }
    if (this.marketHubConnection) {
      await this.marketHubConnection.stop();
    }
    this.logger.info('SignalR connections disconnected');
  }

  isConnected(): boolean {
    return (
      this.userHubConnection?.state === signalR.HubConnectionState.Connected &&
      this.marketHubConnection?.state === signalR.HubConnectionState.Connected
    );
  }
}