Data Provider REST & WebSocket API Documentation
High-performance Binance AggTrades data provider with unified REST API and WebSocket streaming for external developers.
Base URL
http://localhost:8080/api/v1
WebSocket URL
ws://localhost:8080/ws/stream
Response Format
All REST API responses follow a consistent format:
json{
  "success": true,
  "data": { ... },
  "message": "Description of operation result",
  "timestamp": 1640995200000,
  "error": null
}
Security & Access Control
IP Whitelist
Access is restricted to whitelisted IP addresses only. Contact administrators to add your IP to the allowed list.
Rate Limiting

Default: 1200 requests per minute per IP
Burst capacity: Additional requests allowed for short periods
WebSocket: 10 concurrent connections per IP maximum


WebSocket Streaming API
Real-time OHLCV (Open, High, Low, Close, Volume) data streaming with custom timeframes.
Connection
javascriptconst ws = new WebSocket('ws://localhost:8080/ws/stream');
Subscription Protocol
Subscribe to specific symbols and timeframes with optional historical data:
json{
  "action": "subscribe",
  "subscriptions": [
    {
      "symbol": "BTCUSDT",
      "timeframes": [
        {"timeframe": 15, "initial_candles": 20},
        {"timeframe": 173, "initial_candles": 5}
      ]
    },
    {
      "symbol": "ETHUSDT", 
      "timeframes": [
        {"timeframe": 5, "initial_candles": 50},
        {"timeframe": 120, "initial_candles": 10}
      ]
    }
  ]
}
Timeframe Format: All timeframes specified in minutes (e.g., 15 = 15 minutes, 173 = 173 minutes)
Custom Timeframes: Any minute interval supported (1m, 7m, 183m, 2850m, etc.)
Initial Candles: Number of historical candles to receive immediately upon subscription (0-1000)
Message Types
1. Subscription Result
json{
  "type": "subscription_result",
  "successful_subscriptions": {
    "BTCUSDT": [
      {"timeframe": 15, "initial_candles": 20},
      {"timeframe": 173, "initial_candles": 5}
    ]
  },
  "failed_subscriptions": [],
  "message": "All subscriptions successful"
}
2. Initial Historical Data
json{
  "type": "initial_history",
  "symbol": "BTCUSDT",
  "timeframe": "15m",
  "requested_candles": 20,
  "actual_candles": 20,
  "data_status": {
    "historical_loading": false,
    "gap_recovery": false,
    "data_quality": "complete",
    "last_complete_timestamp": null
  },
  "candles": [
    {
      "candle_time": 1735689300000,
      "open": 43245.10,
      "high": 43267.80,
      "low": 43198.20,
      "close": 43250.50,
      "volume": 15.67542,
      "trades_count": 156,
      "taker_buy_volume": 8.23541,
      "taker_sell_volume": 7.44001
    }
  ]
}
3. Real-Time OHLCV Updates
json{
  "type": "ohlcv_update",
  "symbol": "BTCUSDT",
  "timeframe": "15m",
  "data": {
    "candle_time": 1735691100000,
    "open": 43255.30,
    "high": 43268.90,
    "low": 43251.20,
    "close": 43262.40,
    "volume": 12.45332,
    "trades_count": 142,
    "taker_buy_volume": 6.12441,
    "taker_sell_volume": 6.32891
  },
  "data_status": {
    "historical_loading": false,
    "gap_recovery": false,
    "data_quality": "complete",
    "last_complete_timestamp": null
  }
}
4. Data Status Information
All WebSocket messages (initial_history and ohlcv_update) include a data_status field indicating the current state of data collection for the symbol.
Data Quality States:

"loading" - Historical trades are being downloaded from Binance. data will be null.
"partial" - Gap recovery in progress after WebSocket reconnection. data may be incomplete.
"complete" - All data is current and complete. Normal operation.
"unknown" - Unable to determine status (rare error state).

Data Status Fields:

historical_loading - true when downloading historical trades from Binance API
gap_recovery - true when filling gaps after WebSocket disconnection
data_quality - Current quality state (see above)
last_complete_timestamp - Last known complete aggregate_id (only during loading/partial)

Client Behavior Guidelines:

During "loading" or "partial": Display loading indicator, data is not reliable yet
During "complete": Normal operation, data is fully reliable
If you subscribe during "loading": You'll receive empty initial_history first, then a second initial_history with real data when loading completes automatically

Example during data loading:
json{
  "type": "ohlcv_update",
  "symbol": "NEWUSDT",
  "timeframe": "15m",
  "data": null,
  "data_status": {
    "historical_loading": true,
    "gap_recovery": false,
    "data_quality": "loading",
    "last_complete_timestamp": 1234567
  }
}
Example during gap recovery:
json{
  "type": "ohlcv_update",
  "symbol": "BTCUSDT",
  "timeframe": "15m",
  "data": null,
  "data_status": {
    "historical_loading": false,
    "gap_recovery": true,
    "data_quality": "partial",
    "last_complete_timestamp": 9876543
  }
}
5. Error Messages
json{
  "type": "error",
  "error_code": "INVALID_SUBSCRIPTION",
  "message": "Invalid timeframe: timeframe must be positive",
  "details": {}
}
6. Connection Health
json{
  "type": "heartbeat",
  "timestamp": 1735691100000,
  "connections_count": 5
}
Unsubscription
json{
  "action": "unsubscribe",
  "unsubscriptions": [
    {
      "symbol": "BTCUSDT",
      "timeframes": [15, 173]
    }
  ]
}
Ping/Pong
json// Send ping
{
  "action": "ping",
  "timestamp": 1735691100000
}

// Receive pong
{
  "type": "pong", 
  "timestamp": 1735691100500,
  "client_timestamp": 1735691100000
}
WebSocket Features

Real-time updates: 1-second interval for all subscribed timeframes
Custom timeframes: Any minute interval (1m, 173m, 2850m, etc.)
Historical data: Up to 1000 initial candles per subscription
Automatic materialized views: Custom timeframes created automatically
Connection management: Automatic cleanup when clients disconnect
Per-symbol isolation: Subscribe only to needed tickers
Data status tracking: Real-time information about data collection state
Delayed historical delivery: Automatic delivery of historical data after loading completes


REST API Endpoints
Trading Data Endpoints
Get Trades for Symbol
Retrieve historical trades for a specific trading symbol.
Endpoint: GET /data/trades/{symbol}
Parameters:

symbol (path) - Trading symbol (e.g., "BTCUSDT")
start_time (query, optional) - Start timestamp in milliseconds
end_time (query, optional) - End timestamp in milliseconds
limit (query, optional) - Max trades to return (1-10000, default: 1000)
from_id (query, optional) - Start from specific aggregate_id

Example Request:
bashcurl "http://localhost:8080/api/v1/data/trades/BTCUSDT?start_time=1640995200000&limit=500"
Example Response:
json{
  "success": true,
  "data": [
    {
      "aggregate_id": 26129,
      "price": 50000.50,
      "quantity": 0.01,
      "first_trade_id": 27781,
      "last_trade_id": 27781,
      "timestamp": 1640995200153,
      "is_buyer_maker": true
    }
  ],
  "message": "Retrieved 500 trades for BTCUSDT",
  "timestamp": 1640995200000
}
Get Latest Trades
Retrieve the most recent trades for a symbol.
Endpoint: GET /data/trades/{symbol}/latest
Parameters:

symbol (path) - Trading symbol
limit (query, optional) - Number of latest trades (1-1000, default: 100)


System Monitoring Endpoints
Health Check
Get comprehensive system health status.
Endpoint: GET /monitoring/health
Example Response:
json{
  "status": "healthy",
  "components": {
    "initialized": true,
    "binance_client": {"connected": true},
    "weight_coordinator": {"active": true, "healthy": true},
    "tickers": {"active_tasks": 25, "healthy_processors": 25},
    "websocket_manager": {"connections": 3, "active_symbols": 5}
  },
  "timestamp": 1640995200000,
  "uptime_seconds": 3600.5
}
System Information
Endpoint: GET /monitoring/system/info

Ticker Management Endpoints
Get All Active Tickers
Endpoint: GET /tickers/
Get Ticker Information
Endpoint: GET /tickers/{symbol}
Get Materialized Views for Ticker
List all available timeframe materialized views for a ticker.
Endpoint: GET /tickers/{symbol}/mvs
Example Response:
json{
  "success": true,
  "data": [
    {
      "name": "ohlcv_15_btcusdt_mv",
      "timeframe_minutes": 15,
      "total_rows": 50000,
      "total_bytes": 2048576
    },
    {
      "name": "ohlcv_173_btcusdt_mv", 
      "timeframe_minutes": 173,
      "total_rows": 4500,
      "total_bytes": 184320
    }
  ],
  "message": "Retrieved 2 materialized views for BTCUSDT",
  "timestamp": 1640995200000
}

Administrative Endpoints
WebSocket Manager Status
Get WebSocket manager statistics and status.
Endpoint: GET /admin/websocket/status
Example Response:
json{
  "success": true,
  "data": {
    "connections_count": 3,
    "active_symbols": 5,
    "total_subscriptions": 12,
    "mv_references_count": 8,
    "clickhouse_managers": 5,
    "messages_sent": 15420,
    "errors_count": 2,
    "uptime_seconds": 3600,
    "update_task_running": true,
    "heartbeat_task_running": true
  },
  "message": "WebSocket status retrieved successfully",
  "timestamp": 1640995200000
}
Active WebSocket Connections
Get detailed information about active WebSocket connections.
Endpoint: GET /admin/websocket/connections
Example Response:
json{
  "success": true,
  "data": {
    "connections": [
      {
        "client_id": "127.0.0.1_140234567890",
        "ip_address": "127.0.0.1",
        "connected_at": 1735685400.123,
        "connection_duration": 5700.5,
        "last_ping": 1735691070.456,
        "subscriptions": {
          "BTCUSDT": [15, 60, 173],
          "ETHUSDT": [5, 240]
        },
        "total_subscriptions": 5
      }
    ],
    "total_connections": 1,
    "active_symbols": ["BTCUSDT", "ETHUSDT"],
    "mv_references": {
      "BTCUSDT:15m": 1,
      "BTCUSDT:60m": 1,
      "BTCUSDT:173m": 1,
      "ETHUSDT:5m": 1,
      "ETHUSDT:240m": 1
    }
  },
  "message": "WebSocket connections info retrieved successfully",
  "timestamp": 1640995200000
}
WeightCoordinator Statistics
Endpoint: GET /admin/coordinators/weight
Force System Synchronization
Endpoint: POST /admin/system/sync

Error Handling
HTTP Status Codes

200 - Success
400 - Bad Request (invalid parameters)
403 - Forbidden (IP not whitelisted)
404 - Not Found (symbol not active)
429 - Too Many Requests (rate limited)
500 - Internal Server Error
503 - Service Unavailable

WebSocket Error Codes

INVALID_JSON - Malformed JSON message
UNKNOWN_ACTION - Unsupported action type
INVALID_SUBSCRIPTION - Invalid subscription format
INVALID_TIMEFRAME - Invalid timeframe value
CONNECTION_ERROR - Connection-related error
TIMEOUT - Connection timeout


Integration Examples
JavaScript WebSocket Client
javascriptclass DataProviderClient {
  constructor(url) {
    this.ws = new WebSocket(url);
    this.setupEventHandlers();
  }

  setupEventHandlers() {
    this.ws.onopen = () => {
      console.log('Connected to DataProvider WebSocket');
      this.subscribe([
        {
          symbol: 'BTCUSDT',
          timeframes: [
            {timeframe: 15, initial_candles: 50},
            {timeframe: 60, initial_candles: 24}
          ]
        }
      ]);
    };

    this.ws.onmessage = (event) => {
      const message = JSON.parse(event.data);
      this.handleMessage(message);
    };

    this.ws.onerror = (error) => {
      console.error('WebSocket error:', error);
    };

    this.ws.onclose = () => {
      console.log('WebSocket connection closed');
    };
  }

  subscribe(subscriptions) {
    this.ws.send(JSON.stringify({
      action: 'subscribe',
      subscriptions: subscriptions
    }));
  }

  handleMessage(message) {
    switch(message.type) {
      case 'initial_history':
        console.log(`Received ${message.actual_candles} historical candles for ${message.symbol} ${message.timeframe}`);
        this.processHistoricalData(message);
        break;
        
      case 'ohlcv_update':
        // Check data status before processing
        if (message.data_status.data_quality === 'complete') {
          console.log(`Real-time update: ${message.symbol} ${message.timeframe}`, message.data);
          this.processRealtimeUpdate(message);
        } else {
          console.log(`Data loading for ${message.symbol}: ${message.data_status.data_quality}`);
          this.showLoadingIndicator(message.symbol, message.data_status);
        }
        break;
        
      case 'subscription_result':
        console.log('Subscription result:', message);
        break;
        
      case 'error':
        console.error(`WebSocket error [${message.error_code}]: ${message.message}`);
        break;
    }
  }

  processHistoricalData(message) {
    // Initialize charts with historical data
    if (message.data_status.data_quality === 'complete' && message.candles.length > 0) {
      message.candles.forEach(candle => {
        // Process each historical candle
      });
    }
  }

  processRealtimeUpdate(message) {
    // Update charts with real-time data
    const candle = message.data;
    // Update your UI with the latest candle data
  }

  showLoadingIndicator(symbol, dataStatus) {
    // Show loading indicator in UI
    if (dataStatus.historical_loading) {
      console.log(`Downloading historical data for ${symbol}...`);
    } else if (dataStatus.gap_recovery) {
      console.log(`Recovering gaps for ${symbol}...`);
    }
  }
}

// Usage
const client = new DataProviderClient('ws://localhost:8080/ws/stream');
Python WebSocket Client
pythonimport asyncio
import websockets
import json

class DataProviderClient:
    def __init__(self, uri):
        self.uri = uri
        
    async def connect(self):
        async with websockets.connect(self.uri) as websocket:
            # Subscribe to data
            subscription = {
                "action": "subscribe",
                "subscriptions": [
                    {
                        "symbol": "BTCUSDT",
                        "timeframes": [
                            {"timeframe": 15, "initial_candles": 100},
                            {"timeframe": 173, "initial_candles": 20}
                        ]
                    }
                ]
            }
            await websocket.send(json.dumps(subscription))
            
            # Listen for messages
            async for message in websocket:
                data = json.loads(message)
                await self.handle_message(data)
    
    async def handle_message(self, message):
        msg_type = message.get('type')
        
        if msg_type == 'initial_history':
            data_status = message.get('data_status', {})
            if data_status.get('data_quality') == 'complete':
                print(f"Received {len(message['candles'])} historical candles for {message['symbol']} {message['timeframe']}")
            else:
                print(f"Initial history loading for {message['symbol']}: {data_status.get('data_quality')}")
            
        elif msg_type == 'ohlcv_update':
            data_status = message.get('data_status', {})
            
            if data_status.get('data_quality') == 'complete' and message.get('data'):
                candle = message['data']
                print(f"Real-time: {message['symbol']} {message['timeframe']} - Close: {candle['close']}")
            else:
                print(f"Data not ready for {message['symbol']}: {data_status.get('data_quality')}")
            
        elif msg_type == 'error':
            print(f"Error [{message['error_code']}]: {message['message']}")

# Usage
async def main():
    client = DataProviderClient('ws://localhost:8080/ws/stream')
    await client.connect()

asyncio.run(main())
Combined REST + WebSocket Example
javascriptclass TradingDataProvider {
  constructor(apiBase, wsUrl) {
    this.apiBase = apiBase;
    this.wsUrl = wsUrl;
    this.ws = null;
  }

  // Get historical data via REST
  async getHistoricalTrades(symbol, hours = 24) {
    const endTime = Date.now();
    const startTime = endTime - (hours * 60 * 60 * 1000);
    
    const response = await fetch(
      `${this.apiBase}/data/trades/${symbol}?start_time=${startTime}&limit=5000`
    );
    
    const data = await response.json();
    if (!data.success) throw new Error(data.error);
    
    return data.data;
  }

  // Get real-time data via WebSocket  
  connectRealtime(symbols) {
    this.ws = new WebSocket(this.wsUrl);
    
    this.ws.onopen = () => {
      const subscriptions = symbols.map(symbol => ({
        symbol: symbol,
        timeframes: [{timeframe: 1, initial_candles: 0}] // Only real-time
      }));
      
      this.ws.send(JSON.stringify({
        action: 'subscribe',
        subscriptions: subscriptions
      }));
    };
    
    this.ws.onmessage = (event) => {
      const message = JSON.parse(event.data);
      if (message.type === 'ohlcv_update') {
        this.onRealtimeUpdate(message);
      }
    };
  }

  onRealtimeUpdate(message) {
    // Override this method to handle real-time updates
    console.log('Real-time update:', message);
  }
}

// Usage: Historical data via REST, real-time via WebSocket
const provider = new TradingDataProvider(
  'http://localhost:8080/api/v1',
  'ws://localhost:8080/ws/stream'
);

// Get historical trades
provider.getHistoricalTrades('BTCUSDT', 1).then(trades => {
  console.log(`Loaded ${trades.length} historical trades`);
});

// Connect for real-time updates
provider.onRealtimeUpdate = (message) => {
  if (message.data_status.data_quality === 'complete' && message.data) {
    console.log(`${message.symbol}: Price ${message.data.close}, Volume ${message.data.volume}`);
  }
};

provider.connectRealtime(['BTCUSDT', 'ETHUSDT']);

Best Practices
WebSocket Usage

Connection Management: Implement reconnection logic for production use
Selective Subscriptions: Subscribe only to needed symbols and timeframes
Initial Candles: Request appropriate amount of historical data (avoid excessive initial_candles)
Error Handling: Always handle error messages and connection failures
Ping/Pong: Implement periodic ping to maintain connection health
Data Status Monitoring: Always check data_status.data_quality before processing data
Loading States: Display appropriate UI indicators during loading and partial states
Delayed History: Handle automatic delivery of historical data after initial subscription

REST API Usage

Rate Limiting: Respect rate limits to avoid being blocked
Efficient Queries: Use appropriate time ranges and limits
Caching: Cache frequently accessed data locally
Error Handling: Implement retry logic with exponential backoff

Performance Optimization

Use WebSocket for real-time data instead of polling REST endpoints
Combine historical (REST) + real-time (WebSocket) for complete data coverage
Custom timeframes allow precise data analysis without client-side aggregation
Materialized views provide instant access to pre-calculated OHLCV data
Status-aware processing prevents unnecessary computations on incomplete data


Interactive Documentation
OpenAPI (Swagger) documentation is available at:

Swagger UI: http://localhost:8080/docs
ReDoc: http://localhost:8080/redoc


Support & Contact
For technical support, API access requests, or questions:

Check system health: GET /monitoring/health
Monitor WebSocket status: GET /admin/websocket/status
Contact development team for IP whitelist requests