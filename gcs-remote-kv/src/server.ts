import express from 'express'
import cors from 'cors'
import { Storage } from '@google-cloud/storage'

const app = express()
app.use(cors())
app.use(express.json({ limit: '1mb' }))

const PORT = process.env.PORT || 8080
const BUCKET_NAME = process.env.BUCKET_NAME || ''
const AUTH_TOKEN = process.env.AUTH_TOKEN || ''
const KEY_PREFIX = process.env.KEY_PREFIX || ''

if (!BUCKET_NAME) {
  // Critical configuration
  console.error('BUCKET_NAME is required')
  process.exit(1)
}

const storage = new Storage()
const bucket = storage.bucket(BUCKET_NAME)

app.get('/healthz', (_req, res) => res.status(200).send('ok'))

app.use((req, res, next) => {
  if (!AUTH_TOKEN) return next()
  const auth = req.get('authorization') || ''
  const token = auth.startsWith('Bearer ') ? auth.slice(7) : ''
  if (token !== AUTH_TOKEN) return res.status(401).json({ error: 'unauthorized' })
  next()
})

const buildPrefix = (user?: string) => {
  if (user && user.length > 0) return `${KEY_PREFIX}${encodeURIComponent(user)}/`
  return KEY_PREFIX
}

const objName = (user: string | undefined, key: string) => {
  return `${buildPrefix(user)}${key}`
}

app.get('/get', async (req, res) => {
  try {
    const key = String(req.query.key || '')
    const user = req.query.user ? String(req.query.user) : undefined
    if (!key) return res.status(400).json({ error: 'key required' })
    const file = bucket.file(objName(user, key))
    const [exists] = await file.exists()
    if (!exists) return res.status(404).json({ error: 'not_found' })
    const [buf] = await file.download()
    const value = buf.toString('utf8')
    if ((req.get('accept') || '').includes('text/plain')) return res.type('text/plain').send(value)
    res.json({ value })
  } catch (e) {
    res.status(500).json({ error: 'internal' })
  }
})

app.post('/set', async (req, res) => {
  try {
    const key = String(req.body?.key || '')
    const value = req.body?.value
    const user = req.body?.user ? String(req.body.user) : undefined
    if (!key || typeof value !== 'string') return res.status(400).json({ error: 'key and string value required' })
    const file = bucket.file(objName(user, key))
    await file.save(value, { contentType: 'text/plain', resumable: false, cacheControl: 'no-store' })
    res.json({ ok: true })
  } catch (e) {
    res.status(500).json({ error: 'internal' })
  }
})

app.delete('/delete', async (req, res) => {
  try {
    const key = String(req.query.key || '')
    const user = req.query.user ? String(req.query.user) : undefined
    if (!key) return res.status(400).json({ error: 'key required' })
    const file = bucket.file(objName(user, key))
    await file.delete({ ignoreNotFound: true })
    res.json({ ok: true })
  } catch (e) {
    res.status(500).json({ error: 'internal' })
  }
})

app.get('/list', async (req, res) => {
  try {
    const prefix = String(req.query.prefix || '')
    const user = req.query.user ? String(req.query.user) : undefined
    const base = buildPrefix(user)
    const gcsPrefix = `${base}${prefix}`
    const [files] = await bucket.getFiles({ prefix: gcsPrefix, autoPaginate: true })
    const keys = files
      .map(f => f.name)
      .filter(name => name.startsWith(base))
      .map(name => name.slice(base.length))
      .filter(name => name.length > 0)
    res.json({ keys })
  } catch (e) {
    res.status(500).json({ error: 'internal' })
  }
})

app.listen(Number(PORT), () => {
  console.log(`gcs-remote-kv listening on :${PORT}`)
})
