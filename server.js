// ====================================================================
// Void AI Matrix (SnapDeploy & PaaS Precision Fixed Edition)
// ====================================================================

const http = require('http');
const net = require('net');
const https = require('https');
const { WebSocketServer } = require('ws');

const ENV = {
    // 【修复1】加上 .trim()，彻底解决从面板复制粘贴带来的空格/换行符问题
    S_TKN: (process.env.UUID || '2523c510-9ff0-415b-9582-93949bfae7e3').trim(),
    S_ORIGIN: 'https://www.microsoft.com',
    PORT: process.env.PORT || 3000,
    // 【修复2】将 VLESS 专用路径从根目录独立出来，绕过网关的根目录阻断
    WS_PATH: '/stream'
};

process.on('uncaughtException', (err) => console.error('[System] Uncaught Exception:', err.message));
process.on('unhandledRejection', (reason) => console.error('[System] Unhandled Rejection:', reason));

const Registry = new Map();

const initToken = () => {
    const b = new Uint8Array(16);
    const h = c => (c > 64 ? c + 9 : c) & 0xF;
    for (let i = 0, p = 0; i < 16; i++) {
        let c = ENV.S_TKN.charCodeAt(p++); if (c === 45) c = ENV.S_TKN.charCodeAt(p++);
        const hi = h(c); c = ENV.S_TKN.charCodeAt(p++); if (c === 45) c = ENV.S_TKN.charCodeAt(p++);
        b[i] = (hi << 4) | h(c);
    }
    return b;
};

const _S_KEY = initToken();

Registry.set('validate', (buf) => {
    for (let i = 0; i < 16; i++) if (buf[i + 1] !== _S_KEY[i]) return false;
    return true;
});

Registry.set('resolve', (type, buf) => {
    if (type === 1) return `${buf[0]}.${buf[1]}.${buf[2]}.${buf[3]}`;
    if (type === 3) return buf.toString('utf8');
    const ipv6 = [];
    for (let i = 0; i < 8; i++) ipv6.push(((buf[i * 2] << 8) | buf[i * 2 + 1]).toString(16));
    return ipv6.join(':');
});

Registry.set('parse', (buf) => {
    if (buf.length < 24 || !Registry.get('validate')(buf)) return null;
    const mLen = buf[17];
    const prot = buf[18 + mLen];
    const port = (buf[19 + mLen] << 8) | buf[20 + mLen];
    let type = buf[21 + mLen]; if (type !== 1) type += 1;
    let aLen = 0, aOff = 22 + mLen;
    if (type === 3) { aLen = buf[aOff]; aOff++; }
    else if (type === 1) aLen = 4;
    else if (type === 4) aLen = 16;
    const pOff = aOff + aLen;
    if (pOff > buf.length) return null;
    return { prot, type, port, clusterId: buf.subarray(aOff, pOff), pOff };
});

const renderAIMatrix = (res) => {
    const html = `
    <!DOCTYPE html>
    <html lang="en">
    <head>
        <meta charset="utf-8">
        <meta name="viewport" content="width=device-width, initial-scale=1">
        <title>LLM Edge Tensor Node</title>
        <script src="https://cdn.tailwindcss.com"></script>
        <style>
            @import url('https://fonts.googleapis.com/css2?family=JetBrains+Mono:wght@400;700&display=swap');
            body { background: #050505; color: #e2e8f0; font-family: 'JetBrains Mono', monospace; }
            .grid-bg { background-image: linear-gradient(rgba(25, 25, 30, 0.5) 1px, transparent 1px), linear-gradient(90deg, rgba(25, 25, 30, 0.5) 1px, transparent 1px); background-size: 30px 30px; }
            .glow-text { text-shadow: 0 0 10px rgba(56, 189, 248, 0.6); }
        </style>
    </head>
    <body class="grid-bg min-h-screen p-12 flex flex-col items-center justify-center">
        <h1 class="text-5xl font-bold text-sky-400 glow-text mb-4">AETHER<span class="text-white">_NODE</span></h1>
        <p class="text-gray-500">Distributed Inference Engine [Status: ONLINE]</p>
    </body>
    </html>
    `;
    res.writeHead(200, { 'Content-Type': 'text/html; charset=utf-8', 'Cache-Control': 'no-store' });
    res.end(html);
};

const mirrorOrigin = (req, res) => {
    const options = {
        hostname: new URL(ENV.S_ORIGIN).hostname,
        port: 443, path: req.url, method: req.method,
        headers: { ...req.headers, host: new URL(ENV.S_ORIGIN).hostname }
    };
    const proxyReq = https.request(options, proxyRes => {
        res.writeHead(proxyRes.statusCode, proxyRes.headers);
        proxyRes.pipe(res, { end: true });
    });
    proxyReq.on('error', () => { res.writeHead(503); res.end(); });
    req.pipe(proxyReq, { end: true });
};

const generateSubscription = (req, res) => {
    const host = req.headers.host;
    const tag = encodeURIComponent('SnapDeploy-Node');
    // 【修复2配套】订阅链接的 Path 修改为独立的 /stream
    const tB64 = "dmxlc3M6Ly97aWR9QHtob3N0fTo0NDM/ZW5jcnlwdGlvbj1ub25lJnNlY3VyaXR5PXRscyZzbmk9e2hvc3R9JmZwPWNocm9tZSZ0eXBlPXdzJmhvc3Q9e2hvc3R9JnBhdGg9JTJGc3RyZWFtI3t0YWd9";
    const link = Buffer.from(tB64, 'base64').toString('utf8').replace('{id}', ENV.S_TKN).replace(/{host}/g, host).replace('{tag}', tag);
    res.writeHead(200, { 'Content-Type': 'text/plain; charset=utf-8', 'Cache-Control': 'no-store' });
    res.end(Buffer.from(link).toString('base64'));
};

const server = http.createServer((req, res) => {
    const url = new URL(req.url, `http://${req.headers.host}`);

    if (req.method === 'GET' && ['/health', '/healthz', '/livez'].includes(url.pathname)) {
        res.writeHead(200, { 'Content-Type': 'text/plain' });
        return res.end('OK');
    }

    if (req.method === 'GET' && url.pathname === '/' + ENV.S_TKN) return generateSubscription(req, res);
    if (req.method === 'GET' && url.pathname === '/') return renderAIMatrix(res);

    const op = Math.random();
    if (op < 0.05) { res.writeHead(503); return res.end('Service Unavailable'); }
    if (op < 0.1) { res.writeHead(403); return res.end('Forbidden'); }

    return mirrorOrigin(req, res);
});

// ====================================================================
// WebSocket 引擎
// ====================================================================

// 【修复3】配置 handleProtocols：如果客户端 V2ray 携带了 Sec-WebSocket-Protocol 进行伪装，必须原封不动回传！
const wss = new WebSocketServer({
    noServer: true,
    handleProtocols: (protocols) => {
        return protocols[0] || false;
    }
});

const DOH_ENDPOINTS = ['aHR0cHM6Ly8xLjEuMS4xL2Rucy1xdWVyeQ==', 'aHR0cHM6Ly9kbnMuZ29vZ2xlL2Rucy1xdWVyeQ==', 'aHR0cHM6Ly85LjkuOS45L2Rucy1xdWVyeQ=='].map(b64 => Buffer.from(b64, 'base64').toString('utf8'));

server.on('upgrade', (request, socket, head) => {
    const url = new URL(request.url, `http://${request.headers.host}`);

    // 【修复2配套】只有访问专属的 /stream 路径，才允许建立 WebSocket，极大增强隐蔽性和防拦截能力
    if (url.pathname !== ENV.WS_PATH) {
        socket.destroy();
        return;
    }

    wss.handleUpgrade(request, socket, head, ws => { wss.emit('connection', ws); });
});

wss.on('connection', ws => {
    let isFirstPacket = true;
    let edgeSocket = null;
    let isUDP = false;
    let udpBuffer = Buffer.alloc(0);

    const processUDP = async () => {
        while (udpBuffer.length >= 2) {
            const len = (udpBuffer[0] << 8) | udpBuffer[1];
            if (udpBuffer.length >= 2 + len) {
                const queryData = udpBuffer.subarray(2, 2 + len);
                udpBuffer = udpBuffer.subarray(2 + len);

                (async () => {
                    for (const endpoint of DOH_ENDPOINTS) {
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
        if (isFirstPacket) {
            isFirstPacket = false;
            const meta = Registry.get('parse')(msg);

            if (!meta) {
                console.log('[Auth] Invalid Payload. Closing WS.');
                setTimeout(() => ws.close(), Math.random() * 300 + 50);
                return;
            }

            ws.send(Buffer.from([msg[0], 0]));

            const initialPayload = msg.subarray(meta.pOff);

            if (meta.prot === 2) {
                isUDP = true;
                if (meta.port !== 53) { ws.close(); return; }
                udpBuffer = initialPayload;
                processUDP();
                return;
            }

            const host = Registry.get('resolve')(meta.type, meta.clusterId);

            edgeSocket = net.createConnection({ host: host, port: meta.port }, () => {
                if (initialPayload.length > 0) edgeSocket.write(initialPayload);
            });

            edgeSocket.on('data', chunk => { if (ws.readyState === ws.OPEN) ws.send(chunk); });
            edgeSocket.on('error', (err) => {
                // 【诊断功能】如果在 SnapDeploy 后台看到这里的报错，说明平台封锁了出站 TCP
                console.error(`[TCP Outbound Error] -> ${host}:${meta.port} | Msg: ${err.message}`);
                ws.close();
            });
            edgeSocket.on('close', () => ws.close());
        } else {
            if (isUDP) {
                udpBuffer = Buffer.concat([udpBuffer, msg]);
                if (udpBuffer.length > 65536) { ws.close(); return; }
                processUDP();
            } else {
                if (edgeSocket && !edgeSocket.destroyed) edgeSocket.write(msg);
            }
        }
    });

    ws.on('close', () => { if (edgeSocket) edgeSocket.destroy(); });
    ws.on('error', () => { if (edgeSocket) edgeSocket.destroy(); });
});

server.listen(ENV.PORT, () => {
    console.log(`[SYSTEM] Aether Matrix Engine ONLINE | Port: ${ENV.PORT} | Path: ${ENV.WS_PATH}`);
});
