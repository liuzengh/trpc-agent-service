// 假企业微信服务器：模拟扫码授权跳转 + gettoken + getuserinfo。
import http from 'node:http'

const PORT = Number(process.env.WECOM_FAKE_PORT ?? 10086)
const USERID = process.env.E2E_WECOM_USERID ?? 'zhangsan'
const TOKEN = 'fake-access-token-e2e'

const server = http.createServer((req, res) => {
  const url = new URL(req.url ?? '/', `http://127.0.0.1:${PORT}`)
  const route = url.pathname

  // 1) gettoken：corpid/corpsecret 校验后可配置化，这里接受任意值。
  if (route === '/cgi-bin/gettoken') {
    const corpid = url.searchParams.get('corpid') ?? ''
    const corpsecret = url.searchParams.get('corpsecret') ?? ''
    if (!corpid || !corpsecret) {
      res.writeHead(400, { 'Content-Type': 'application/json' })
      res.end(JSON.stringify({ errcode: 40029, errmsg: 'invalid corpid or corpsecret' }))
      return
    }
    res.writeHead(200, { 'Content-Type': 'application/json' })
    res.end(JSON.stringify({ errcode: 0, errmsg: 'ok', access_token: TOKEN, expires_in: 7200 }))
    return
  }

  // 2) getuserinfo：任意合法 code 返回固定 userid（E2E_WECOM_USERID）。
  if (route === '/cgi-bin/auth/getuserinfo') {
    const token = url.searchParams.get('access_token') ?? ''
    const code = url.searchParams.get('code') ?? ''
    if (token !== TOKEN || !code) {
      res.writeHead(200, { 'Content-Type': 'application/json' })
      res.end(JSON.stringify({ errcode: 40003, errmsg: 'invalid token or code' }))
      return
    }
    res.writeHead(200, { 'Content-Type': 'application/json' })
    res.end(JSON.stringify({ errcode: 0, errmsg: 'ok', userid: USERID }))
    return
  }

  // 3) 授权页：模拟企微完成授权，302 回 redirect_uri 并带 code+state。
  if (route === '/wwlogin/sso/login') {
    const redirectURI = url.searchParams.get('redirect_uri') ?? ''
    const state = url.searchParams.get('state') ?? ''
    if (!redirectURI) {
      res.writeHead(400, { 'Content-Type': 'text/plain' })
      res.end('missing redirect_uri')
      return
    }
    const separator = redirectURI.includes('?') ? '&' : '?'
    res.writeHead(302, { Location: `${redirectURI}${separator}code=e2e-fake-code&state=${encodeURIComponent(state)}` })
    res.end()
    return
  }

  res.writeHead(404, { 'Content-Type': 'text/plain' })
  res.end('not found')
})

server.listen(PORT, '127.0.0.1', () => {
  console.log(`[fake-wecom] listening on http://127.0.0.1:${PORT} (userid=${USERID})`)
})
