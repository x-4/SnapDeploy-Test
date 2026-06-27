FROM node:20-alpine
WORKDIR /app
COPY package.json ./
RUN npm install
COPY server.js ./

# SnapDeploy 会自动注入 PORT 环境变量
ENV PORT=3000
EXPOSE ${PORT}

CMD ["node", "server.js"]