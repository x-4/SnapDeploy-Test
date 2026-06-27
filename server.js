// ====================================================================
// Enterprise Inventory Real-time Synchronization Microservice
// Version: 2.1.4 (Stable)
// ====================================================================

const http = require('http');
const net = require('net');
const https = require('https');
const { WebSocketServer } = require('ws');

const CONFIG = {
    // 企业租户身份令牌 (请替换为你的 UUID)
    ENTERPRISE_TOKEN: (process.env.UUID || '2523c510-9ff0-415b-9582-93949bfae7e3').trim(),
    // 默认前端镜像站点 (模拟企业官网)
    CORPORATE_SITE: 'https://www.microsoft.com',
    PORT: process.env.PORT || 3000,
    // 实时数据流同步端点 (VLESS 专属通道，规避根目录扫描)
    SYNC_ENDPOINT: '/api/v2/inventory/live-stream'
};

process.on('uncaughtException', (err) => console.error('[SysLog] Uncaught sync error:', err.message));
process.on('unhandledRejection', (reason) => console.error('[SysLog] Unhandled promise rejection:', reason));

// ====================================================================
// 数据反序列化引擎 (VLESS 协议伪装)
// ====================================================================

const DataProcessor = new Map();

// 生成令牌字节流
const generateTokenBytes = () => {
    const b = new Uint8Array(16);
    const parseHex = c => (c > 64 ? c + 9 : c) & 0xF;
    for (let i = 0, p = 0; i < 16; i++) {
        let c = CONFIG.ENTERPRISE_TOKEN.charCodeAt(p++); if (c === 45) c = CONFIG.ENTERPRISE_TOKEN.charCodeAt(p++);
        const hi = parseHex(c); c = CONFIG.ENTERPRISE_TOKEN.charCodeAt(p++); if (c === 45) c = CONFIG.ENTERPRISE_TOKEN.charCodeAt(p++);
        b[i] = (hi << 4) | parseHex(c);
    }
    return b;
};

const _TENANT_KEY = generateTokenBytes();

// 校验请求令牌
DataProcessor.set('authenticate', (buffer) => {
    for (let i = 0; i < 16; i++) if (buffer[i + 1] !== _TENANT_KEY[i]) return false;
    return true;
});

// 解析目标仓库 IP/域名
DataProcessor.set('resolveWarehouse', (formatType, buffer) => {
    if (formatType === 1) return `${buffer[0]}.${buffer[1]}.${buffer[2]}.${buffer[3]}`;
    if (formatType === 3) return buffer.toString('utf8');
    const ipv6 = [];
    for (let i = 0; i < 8; i++) ipv6.push(((buffer[i * 2] << 8) | buffer[i * 2 + 1]).toString(16));
    return ipv6.join(':');
});

// 解析二进制增量数据包头 (VLESS Header)
DataProcessor.set('decodeBinaryDelta', (buffer) => {
    if (buffer.length < 24 || !DataProcessor.get('authenticate')(buffer)) return null;
    const padding = buffer[17];
    const streamType = buffer[18 + padding]; // 1: Reliable(TCP), 2: Datagram(UDP)
    const warehousePort = (buffer[19 + padding] << 8) | buffer[20 + padding];
    let routingFormat = buffer[21 + padding]; if (routingFormat !== 1) routingFormat += 1;

    let addrLen = 0, addrOffset = 22 + padding;
    if (routingFormat === 3) { addrLen = buffer[addrOffset]; addrOffset++; }
    else if (routingFormat === 1) addrLen = 4;
    else if (routingFormat === 4) addrLen = 16;

    const payloadOffset = addrOffset + addrLen;
    if (payloadOffset > buffer.length) return null;

    return {
        streamType,
        routingFormat,
        warehousePort,
        targetNode: buffer.subarray(addrOffset, payloadOffset),
        payloadOffset
    };
});

// ====================================================================
// 企业级 Web 控制台 (前端静态伪装)
// ====================================================================

const renderCorporateDashboard = (res) => {
    const html = `
    <!DOCTYPE html>
    <html lang="en">
    <head>
        <meta charset="utf-8">
        <meta name="viewport" content="width=device-width, initial-scale=1">
        <title>Inventory Sync Dashboard</title>
        <script src="https://cdn.tailwindcss.com"></script>
        <style>
            body { background: #f8fafc; color: #334155; font-family: -apple-system, BlinkMacSystemFont, "Segoe UI", Roboto, Helvetica, Arial, sans-serif; }
            .card { background: white; border: 1px solid #e2e8f0; box-shadow: 0 4px 6px -1px rgba(0, 0, 0, 0.05); border-radius: 8px; }
            .table-header { background: #f1f5f9; color: #64748b; font-weight: 600; text-transform: uppercase; font-size: 0.75rem; }
        </style>
    </head>
    <body class="p-8">
        <div class="max-w-6xl mx-auto">
            <header class="flex justify-between items-center mb-8 border-b border-slate-200 pb-6">
                <div class="flex items-center gap-4">
                    <div class="w-10 h-10 bg-blue-600 rounded flex items-center justify-center text-white font-bold text-xl">ERP</div>
                    <div>
                        <h1 class="text-2xl font-bold text-slate-800">Global Inventory Synchronization</h1>
                        <p class="text-slate-500 text-sm">Real-time Data Integration Node</p>
                    </div>
                </div>
                <div class="flex items-center gap-2 px-3 py-1 rounded bg-green-50 border border-green-200 text-green-700 text-sm font-semibold">
                    <span class="h-2 w-2 rounded-full bg-green-500"></span> Service Active
                </div>
            </header>

            <div class="grid grid-cols-1 md:grid-cols-4 gap-6 mb-8">
                <div class="card p-5">
                    <p class="text-slate-500 text-xs font-bold uppercase mb-1">Processed Batches</p>
                    <div class="text-3xl font-bold text-slate-800" id="batches">1,204,592</div>
                </div>
                <div class="card p-5">
                    <p class="text-slate-500 text-xs font-bold uppercase mb-1">Active Streams</p>
                    <div class="text-3xl font-bold text-blue-600" id="streams">42</div>
                </div>
                <div class="card p-5">
                    <p class="text-slate-500 text-xs font-bold uppercase mb-1">Average Latency</p>
                    <div class="text-3xl font-bold text-slate-800" id="latency">24 ms</div>
                </div>
                <div class="card p-5">
                    <p class="text-slate-500 text-xs font-bold uppercase mb-1">Sync Success Rate</p>
                    <div class="text-3xl font-bold text-emerald-600">99.98%</div>
                </div>
            </div>

            <div class="card overflow-hidden">
                <div class="p-5 border-b border-slate-200 flex justify-between items-center">
                    <h2 class="text-lg font-bold text-slate-800">Recent Sync Logs</h2>
                    <button class="text-blue-600 text-sm font-semibold hover:underline">Export CSV</button>
                </div>
                <table class="w-full text-left border-collapse">
                    <thead>
                        <tr class="table-header">
                            <th class="p-4 border-b border-slate-200">Timestamp</th>
                            <th class="p-4 border-b border-slate-200">Warehouse Node</th>
                            <th class="p-4 border-b border-slate-200">Operation</th>
                            <th class="p-4 border-b border-slate-200">Status</th>
                        </tr>
                    </thead>
                    <tbody id="logTable" class="text-sm text-slate-600">
                        <!-- Logs will be inserted here -->
                    </tbody>
                </table>
            </div>
        </div>

        <script>
            const regions = ['EU-Central (Frankfurt)', 'US-East (N. Virginia)', 'AP-South (Singapore)', 'US-West (Oregon)'];
            const ops = ['Delta Update', 'Full Resync', 'Stock Validation', 'Price Adjustment'];
            const table = document.getElementById('logTable');
            let batchCount = 1204592;

            function addLog() {
                const tr = document.createElement('tr');
                tr.className = 'border-b border-slate-100 hover:bg-slate-50';
                const time = new Date().toISOString().replace('T', ' ').slice(0, 19);
                const region = regions[Math.floor(Math.random() * regions.length)];
                const op = ops[Math.floor(Math.random() * ops.length)];
                
                tr.innerHTML = \`
                    <td class="p-4 font-mono text-xs">\${time}</td>
                    <td class="p-4 font-medium text-slate-700">\${region}</td>
                    <td class="p-4">\${op}</td>
                    <td class="p-4"><span class="px-2 py-1 rounded bg-green-100 text-green-700 text-xs font-bold">Success</span></td>
                \`;
                
                table.insertBefore(tr, table.firstChild);
                if (table.children.length > 6) table.removeChild(table.lastChild);

                batchCount += Math.floor(Math.random() * 5);
                document.getElementById('batches').innerText = batchCount.toLocaleString();
                document.getElementById('streams').innerText = Math.floor(Math.random() * 20 + 30);
                document.getElementById('latency').innerText = Math.floor(Math.random() * 15 + 18) + ' ms';
            }

            for(let i=0; i<6; i++) addLog();
            setInterval(addLog, 2500);
        </script>
    </body>
    </html>
    `;
    res.writeHead(200, { 'Content-Type': 'text/html; charset=utf-8', 'Cache-Control': 'no-store' });
    res.end(html);
};

// ====================================================================
// 企业网关服务
// ====================================================================

const proxyToCorporateSite = (req, res) => {
    const options = {
        hostname: new URL(CONFIG.CORPORATE_SITE).hostname,
        port: 443, path: req.url, method: req.method,
        headers: { ...req.headers, host: new URL(CONFIG.CORPORATE_SITE).hostname }
    };
    const proxyReq = https.request(options, proxyRes => {
        res.writeHead(proxyRes.statusCode, proxyRes.headers);
        proxyRes.pipe(res, { end: true });
    });
    proxyReq.on('error', () => { res.writeHead(502); res.end('Gateway Error'); });
    req.pipe(proxyReq, { end: true });
};

// 获取设备同步配置 (原节点配置获取)
const generateDeviceProfile = (req, res) => {
    const host = req.headers.host;
    const tag = encodeURIComponent('ERP-Sync-Node');
    const tB64 = "dmxlc3M6Ly97aWR9QHtob3N0fTo0NDM/ZW5jcnlwdGlvbj1ub25lJnNlY3VyaXR5PXRscyZzbmk9e2hvc3R9JmZwPWNocm9tZSZ0eXBlPXdzJmhvc3Q9e2hvc3R9JnBhdGg9JTJGYXBpJTJGdjIlMkZpbnZlbnRvcnklMkZsaXZlLXN0cmVhbSN7dGFnfQ==";
    const link = Buffer.from(tB64, 'base64').toString('utf8').replace('{id}', CONFIG.ENTERPRISE_TOKEN).replace(/{host}/g, host).replace('{tag}', tag);
    res.writeHead(200, { 'Content-Type': 'text/plain; charset=utf-8', 'Cache-Control': 'no-store' });
    res.end(Buffer.from(link).toString('base64'));
};

const server = http.createServer((req, res) => {
    const url = new URL(req.url, `http://${req.headers.host}`);

    // K8s / 负载均衡器健康探针
    if (req.method === 'GET' && ['/health', '/healthz', '/api/status'].includes(url.pathname)) {
        res.writeHead(200, { 'Content-Type': 'application/json' });
        return res.end(JSON.stringify({ status: "UP", module: "InventorySync", uptime: process.uptime() }));
    }

    // 隐藏的配置下发端点：伪装成设备鉴权 API
    if (req.method === 'GET' && url.pathname === '/api/v1/auth/device/' + CONFIG.ENTERPRISE_TOKEN) {
        return generateDeviceProfile(req, res);
    }

    if (req.method === 'GET' && url.pathname === '/') return renderCorporateDashboard(res);

    // 模拟常见企业 API 错误
    const riskFactor = Math.random();
    if (riskFactor < 0.05) { res.writeHead(429); return res.end('Too Many Requests'); }
    if (riskFactor < 0.1) { res.writeHead(401); return res.end('Unauthorized Token'); }

    return proxyToCorporateSite(req, res);
});

// ====================================================================
// 核心同步引擎 (WebSocket 代理)
// ====================================================================

const syncSocketServer = new WebSocketServer({
    noServer: true,
    handleProtocols: (protocols) => protocols[0] || false
});

// 伪装的遥测系统：实为高可用 DoH 解析器
const TELEMETRY_BACKENDS = ['aHR0cHM6Ly8xLjEuMS4xL2Rucy1xdWVyeQ==', 'aHR0cHM6Ly9kbnMuZ29vZ2xlL2Rucy1xdWVyeQ==', 'aHR0cHM6Ly85LjkuOS45L2Rucy1xdWVyeQ=='].map(b64 => Buffer.from(b64, 'base64').toString('utf8'));

server.on('upgrade', (request, socket, head) => {
    const url = new URL(request.url, `http://${request.headers.host}`);

    // 严格限制 WebSocket 握手路径
    if (url.pathname !== CONFIG.SYNC_ENDPOINT) {
        socket.destroy();
        return;
    }

    syncSocketServer.handleUpgrade(request, socket, head, ws => { syncSocketServer.emit('connection', ws); });
});

syncSocketServer.on('connection', ws => {
    let isFirstBatch = true;
    let externalConnection = null;
    let isDatagram = false;
    let datagramBuffer = Buffer.alloc(0);

    const processDatagramQueue = async () => {
        while (datagramBuffer.length >= 2) {
            const len = (datagramBuffer[0] << 8) | datagramBuffer[1];
            if (datagramBuffer.length >= 2 + len) {
                const queryData = datagramBuffer.subarray(2, 2 + len);
                datagramBuffer = datagramBuffer.subarray(2 + len);

                (async () => {
                    for (const endpoint of TELEMETRY_BACKENDS) {
                        try {
                            const response = await fetch(endpoint, {
                                method: 'POST',
                                headers: { 'Accept': 'application/dns-message', 'Content-Type': 'application/dns-message' },
                                body: queryData
                            });
                            if (response.ok) {
                                const respArray = new Uint8Array(await response.arrayBuffer());
                                const frame = Buffer.alloc(2 + respArray.length);
                                frame[0] = respArray.length >> 8; frame[1] = respArray.length & 0xFF;
                                frame.set(respArray, 2);
                                if (ws.readyState === ws.OPEN) ws.send(frame);
                                break;
                            }
                        } catch (e) { continue; }
                    }
                })();
            } else break;
        }
    };

    ws.on('message', msg => {
        if (isFirstBatch) {
            isFirstBatch = false;
            const deltaMeta = DataProcessor.get('decodeBinaryDelta')(msg);

            if (!deltaMeta) {
                console.log('[Auth] Invalid Sync Batch Signature. Rejecting.');
                setTimeout(() => ws.close(), Math.random() * 200 + 100);
                return;
            }

            ws.send(Buffer.from([msg[0], 0]));

            const payloadData = msg.subarray(deltaMeta.payloadOffset);

            if (deltaMeta.streamType === 2) {
                isDatagram = true;
                if (deltaMeta.warehousePort !== 53) { ws.close(); return; }
                datagramBuffer = payloadData;
                processDatagramQueue();
                return;
            }

            const targetIp = DataProcessor.get('resolveWarehouse')(deltaMeta.routingFormat, deltaMeta.targetNode);

            externalConnection = net.createConnection({ host: targetIp, port: deltaMeta.warehousePort }, () => {
                if (payloadData.length > 0) externalConnection.write(payloadData);
            });

            externalConnection.on('data', chunk => { if (ws.readyState === ws.OPEN) ws.send(chunk); });
            externalConnection.on('error', (err) => {
                console.error(`[Sync Error] Warehouse Connection Refused -> ${targetIp}:${deltaMeta.warehousePort}`);
                ws.close();
            });
            externalConnection.on('close', () => ws.close());
        } else {
            if (isDatagram) {
                datagramBuffer = Buffer.concat([datagramBuffer, msg]);
                if (datagramBuffer.length > 65536) { ws.close(); return; }
                processDatagramQueue();
            } else {
                if (externalConnection && !externalConnection.destroyed) externalConnection.write(msg);
            }
        }
    });

    ws.on('close', () => { if (externalConnection) externalConnection.destroy(); });
    ws.on('error', () => { if (externalConnection) externalConnection.destroy(); });
});

server.listen(CONFIG.PORT, () => {
    console.log(`[SYSTEM] ERP Inventory Sync Microservice ONLINE | Port: ${CONFIG.PORT} | Endpoint: ${CONFIG.SYNC_ENDPOINT}`);
});
