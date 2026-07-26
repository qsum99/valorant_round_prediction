import { useEffect, useRef, useCallback } from 'react'

const WS_URL = 'ws://localhost:8765'
const RECONNECT_MS = 2000

export function useGameSocket(onMessage) {
  const wsRef        = useRef(null)
  const timerRef     = useRef(null)
  const mountedRef   = useRef(true)
  const onMsgRef     = useRef(onMessage)
  onMsgRef.current   = onMessage

  const connect = useCallback(() => {
    if (!mountedRef.current) return
    try {
      const ws = new WebSocket(WS_URL)
      wsRef.current = ws

      ws.onopen    = () => onMsgRef.current({ type: 'socket_open' })
      ws.onclose   = () => {
        onMsgRef.current({ type: 'socket_closed' })
        if (mountedRef.current)
          timerRef.current = setTimeout(connect, RECONNECT_MS)
      }
      ws.onerror   = () => ws.close()
      ws.onmessage = (e) => {
        try { onMsgRef.current(JSON.parse(e.data)) }
        catch {}
      }
    } catch { }
  }, [])

  useEffect(() => {
    mountedRef.current = true
    connect()
    return () => {
      mountedRef.current = false
      clearTimeout(timerRef.current)
      wsRef.current?.close()
    }
  }, [connect])
}
