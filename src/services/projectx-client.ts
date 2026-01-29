// reverted to Nov3 code with added withTokenRefresh() method
import { ApiService } from './api.service';
import { SignalRService } from './signalr-service';
import { 
  ProjectXConfig, 
  MarketData, 
  Order, 
  Position, 
  Account, 
  BarData, 
  Contract, 
  Quote,
  GatewayQuote,
  GatewayUserOrder,
  GatewayUserPosition,
  GatewayUserTrade,
  GatewayUserAccount
} from '../types';

export class ProjectXClient {
  private apiService: ApiService;
  private signalRService: SignalRService;
  private config: ProjectXConfig;
  private isInitialized: boolean = false;
  private selectedAccountId: number | null = null;
  private _posCache = new Map<string, { ts: number; net: number }>();
  private _posTtlMs = 3000;

  constructor(config: ProjectXConfig) {
    this.config = config;
    this.apiService = new ApiService(config.baseURL);
    this.signalRService = new SignalRService();
  }

  /**
   * Wrap API calls with automatic token refresh on 401.
   * ONLY re-authenticates - never touches isInitialized or selectedAccountId.
   */
  private async withTokenRefresh<T>(apiCall: () => Promise<T>): Promise<T> {
    try {
      return await apiCall();
    } catch (err: any) {
      const msg = err?.message ?? '';
      if (msg.includes('401') || msg.includes('Unauthorized')) {
        console.warn('[ProjectXClient] Token expired, re-authenticating...');
        
        // ONLY re-authenticate - nothing else
        await this.apiService.authenticate({
          userName: this.config.userName,
          apiKey: this.config.apiKey
        });
        
        // Update SignalR with fresh token
        const newToken = this.apiService.getAuthToken();
        if (newToken && this.selectedAccountId) {
          this.signalRService.updateToken(newToken);
          try {
            await this.signalRService.initialize(newToken, this.selectedAccountId);
            console.info('[ProjectXClient] SignalR reinitialized with fresh token');
          } catch (wsErr) {
            console.error('[ProjectXClient] SignalR reinit failed:', wsErr);
          }
        }
        
        // Retry original call
        return await apiCall();
      }
      throw err;
    }
  }

  private async fetchAllAccountsMerged(): Promise<any[]> {
    await this.initialize();
    const live = await this.apiService.searchAccounts({ live: true }).catch(() => ({ accounts: [] }));
    const prac = await this.apiService.searchAccounts({ live: false }).catch(() => ({ accounts: [] }));

    const mergeById = new Map<number, any>();
    for (const a of (live.accounts ?? [])) mergeById.set(a.id, a);
    for (const a of (prac.accounts ?? [])) mergeById.set(a.id, a);

    return Array.from(mergeById.values());
  }

  private isTradableAccount(a: any): boolean {
    if (!a) return false;
    return a.canTrade === true;
  }

  private async ensureActiveAccount(): Promise<void> {
    await this.initialize();
    if (this.selectedAccountId == null) {
      throw new Error('No account selected (selectedAccountId is null)');
    }

    const all = await this.fetchAllAccountsMerged();
    const acct = all.find(a => a.id === this.selectedAccountId);

    console.info('[account:verify]', acct ? {
      id: acct.id,
      number: acct.accountNumber ?? acct.number ?? acct.name,
      canTrade: acct.canTrade,
      isVisible: acct.isVisible,
      simulated: acct.simulated,
      active: acct.active,
      isActive: acct.isActive,
      status: acct.status,
      live: acct.live,
      balance: acct.balance,
    } : { id: this.selectedAccountId, found: false });

    if (!acct) {
      throw new Error(`Selected account unknown (id=${this.selectedAccountId})`);
    }
    if (!this.isTradableAccount(acct)) {
      throw new Error(`Selected account (id=${acct.id}) is not tradable (canTrade=false)`);
    }
  }

  async initialize(): Promise<void> {
    if (!this.isInitialized) {
      await this.apiService.authenticate({
        userName: this.config.userName,
        apiKey: this.config.apiKey
      });
      
      const accountsResponse = await this.apiService.searchAccounts({ live: true });
      if (accountsResponse.accounts.length > 0) {
        this.selectedAccountId = accountsResponse.accounts[0].id;
      }
      
      this.isInitialized = true;
    }
  }

  async getAccounts(): Promise<Account[]> {
    await this.initialize();
    const response = await this.withTokenRefresh(() =>
      this.apiService.searchAccounts({ live: true })
    );
    return response.accounts;
  }

  async getAccount(accountId: string): Promise<Account> {
    await this.initialize();
    const response = await this.withTokenRefresh(() =>
      this.apiService.searchAccounts({ accountNumber: accountId, live: true })
    );
    return response.accounts[0];
  }

  async getMarketData(symbol: string): Promise<MarketData> {
    throw new Error('getMarketData not implemented - use SignalR for real-time data');
  }

  async searchContracts(symbol: string): Promise<Contract[]> {
    await this.initialize();
    const response = await this.withTokenRefresh(() =>
      this.apiService.searchContracts({ 
        searchText: symbol, 
        live: false
      })
    );
    return response.contracts;
  }

  async getQuotes(contractIds: string[]): Promise<Quote[]> {
    throw new Error('getQuotes not implemented - use SignalR for real-time quotes');
  }

  async getBars(contractId: string, timeframe: string, limit: number = 100): Promise<BarData[]> {
    await this.initialize();
    
    const unit = 1;
    const unitNumber = parseInt(timeframe);
    
    const endTime = new Date();
    const startTime = new Date(endTime.getTime() - 24 * 60 * 60 * 1000);

    const request = {
      contractId,
      live: false,
      startTime: startTime.toISOString(),
      endTime: endTime.toISOString(),
      unit: unit,
      unitNumber: unitNumber,
      limit: limit,
      includePartialBar: false
    };

    const response = await this.withTokenRefresh(() =>
      this.apiService.retrieveBars(request)
    );
    return response.bars;
  }

  async getContract(contractId: string): Promise<Contract> {
    await this.initialize();
    return await this.withTokenRefresh(() =>
      this.apiService.searchContractById({ contractId })
    );
  }

  async getOrders(): Promise<Order[]> {
    await this.initialize();
    if (!this.selectedAccountId) throw new Error('No account selected');
    
    const response = await this.withTokenRefresh(() =>
      this.apiService.searchOrders({ accountId: this.selectedAccountId! })
    );
    if (!response.success) throw new Error(response.errorMessage);
    return response.orders;
  }

  async createOrder(orderRequest: {
    contractId: string;
    type: number;
    side: number;
    size: number;
    limitPrice?: number;
    stopPrice?: number;
    trailPrice?: number;
    linkedOrderId?: number;
  }): Promise<number> {
    await this.initialize();
    if (!this.selectedAccountId) throw new Error('No account selected');

    await this.ensureActiveAccount();

    const side = orderRequest.side === 0 ? 0 : 1;

    const payload: any = {
      accountId: this.selectedAccountId,
      contractId: orderRequest.contractId,
      type: 2,
      side,
      size: orderRequest.size,
    };

    console.log('[order->broker]', payload);

    const response = await this.withTokenRefresh(() =>
      this.apiService.placeOrder(payload)
    );
    if (!response.success) throw new Error(response.errorMessage);
    return response.orderId;
  }

  async cancelOrder(orderId: number): Promise<void> {
    await this.initialize();
    if (!this.selectedAccountId) throw new Error('No account selected');
    
    const response = await this.withTokenRefresh(() =>
      this.apiService.cancelOrder({
        accountId: this.selectedAccountId!,
        orderId
      })
    );
    if (!response.success) throw new Error(response.errorMessage);
  }

  async getPositions(): Promise<Position[]> {
    await this.initialize();
    if (!this.selectedAccountId) throw new Error('No account selected');
    
    const response = await this.withTokenRefresh(() =>
      this.apiService.searchOpenPositions({ accountId: this.selectedAccountId! })
    );
    if (!response.success) throw new Error(response.errorMessage);
    return response.positions;
  }

  async closePosition(contractId: string): Promise<void> {
    await this.initialize();
    if (!this.selectedAccountId) throw new Error('No account selected');
    
    const response = await this.withTokenRefresh(() =>
      this.apiService.closePosition({
        accountId: this.selectedAccountId!,
        contractId
      })
    );
    if (!response.success) throw new Error(response.errorMessage);
  }

  async partialClosePosition(contractId: string, size: number): Promise<void> {
    await this.initialize();
    if (!this.selectedAccountId) throw new Error('No account selected');
    if (!Number.isFinite(size) || size <= 0) throw new Error('partialClosePosition: size must be > 0');

    const response = await this.withTokenRefresh(() =>
      this.apiService.partialClosePosition({
        accountId: this.selectedAccountId!,
        contractId,
        size: Math.floor(size)
      })
    );
    if (!response.success) throw new Error(response.errorMessage);
  }

  async getNetPositionSize(contractId: string): Promise<number> {
    await this.initialize();
    if (!this.selectedAccountId) throw new Error('No account selected');

    const resp = await this.withTokenRefresh(() =>
      this.apiService.searchOpenPositions({ accountId: this.selectedAccountId! })
    );
    if (!resp.success) throw new Error(resp.errorMessage);

    const pos = resp.positions.find(p => (p as any).contractId === contractId) as any | undefined;
    if (!pos) return 0;

    const netQuantity   = (pos as any).netQuantity;
    const longQuantity  = (pos as any).longQuantity;
    const shortQuantity = (pos as any).shortQuantity;
    const quantity      = (pos as any).quantity;  

    let net: number | undefined =
      (typeof netQuantity === 'number') ? netQuantity : undefined;

    if (net === undefined) {
      const hasLS = (typeof longQuantity === 'number') || (typeof shortQuantity === 'number');
      if (hasLS) {
        net = (Number(longQuantity) || 0) - (Number(shortQuantity) || 0);
      }
    }

    if (net === undefined && typeof quantity === 'number') {
      net = quantity;
    }

    return Number(net ?? 0);
  }

  async closePositionByQtySafe(contractId: string, requestedSize: number): Promise<{ closed: number; remaining: number }> {
    await this.initialize();
    if (!this.selectedAccountId) throw new Error('No account selected');
    if (!Number.isFinite(requestedSize) || requestedSize <= 0) throw new Error('requestedSize must be > 0');

    const net = await this.getNetPositionSize(contractId);
    const netAbs = Math.abs(net);
    if (netAbs === 0) return { closed: 0, remaining: 0 };

    const size = Math.min(Math.floor(requestedSize), netAbs);
    if (size === 0) return { closed: 0, remaining: netAbs };

    const response = await this.withTokenRefresh(() =>
      this.apiService.partialClosePosition({
        accountId: this.selectedAccountId!,
        contractId,
        size
      })
    );
    if (!response.success) throw new Error(response.errorMessage);

    return { closed: size, remaining: netAbs - size };
  }

  async closeAllQty(contractId: string): Promise<void> {
    const netAbs = Math.abs(await this.getNetPositionSize(contractId));
    if (netAbs > 0) {
      await this.closePositionByQtySafe(contractId, netAbs);
    }
  }

  async getBalance(): Promise<number> {
    const accounts = await this.getAccounts();
    return accounts[0]?.balance || 0;
  }

  async getEquity(): Promise<number> {
    const accounts = await this.getAccounts();
    return accounts[0]?.balance || 0;
  }

  async connectWebSocket(): Promise<void> {
    if (!this.isInitialized) {
      await this.initialize();
    }

    const authToken = this.apiService.getAuthToken();
    if (!authToken || !this.selectedAccountId) {
      throw new Error('Not authenticated or account not selected');
    }

    await this.signalRService.initialize(authToken, this.selectedAccountId);
  }

  async subscribeToSymbols(symbols: string[]): Promise<void> {
    if (!this.signalRService.isConnected()) {
      throw new Error('SignalR service not connected. Call connectWebSocket() first.');
    }

    for (const symbol of symbols) {
      try {
        const contracts = await this.searchContracts(symbol);
        if (contracts.length > 0) {
          await this.signalRService.subscribeToMarketData(contracts[0].id);
        }
      } catch (error) {
        console.error(`Failed to subscribe to ${symbol}:`, error);
      }
    }
  }

  onMarketData(callback: (data: GatewayQuote & { contractId: string }) => void): void {
    this.signalRService.on('market_data', callback);
  }

  onDepth(callback: (data: { contractId: string; timestamp: string; type: number; price: number; volume: number; currentVolume: number }) => void): void {
    (this.signalRService as any).on('market_depth', callback);
  }
  
  onOrderUpdate(callback: (order: GatewayUserOrder) => void): void {
    this.signalRService.on('order_update', callback);
  }

  onPositionUpdate(callback: (position: GatewayUserPosition) => void): void {
    this.signalRService.on('position_update', callback);
  }

  onTradeUpdate(callback: (trade: GatewayUserTrade) => void): void {
    this.signalRService.on('trade_update', callback);
  }

  onAccountUpdate(callback: (account: GatewayUserAccount) => void): void {
    this.signalRService.on('account_update', callback);
  }

  onConnected(callback: () => void): void {
    if (this.signalRService.isConnected()) {
      callback();
    }
  }

  onError(callback: (error: any) => void): void {
    console.warn('Custom error handling not implemented for SignalR');
  }

  async disconnectWebSocket(): Promise<void> {
    await this.signalRService.disconnect();
  }

  isWebSocketConnected(): boolean {
    return this.signalRService.isConnected();
  }

  getAuthToken(): string | null {
    return this.apiService.getAuthToken();
  }

  getSelectedAccountId(): number | null {
    return this.selectedAccountId;
  }

  getSignalRService(): SignalRService {
    return this.signalRService;
  }

  public setSelectedAccountId(id: number): void {
    this.selectedAccountId = id;
    console.log('[account:selected]', { id });
  }
}