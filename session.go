package cfrelaytun

import (
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"net/url"
	"sync"
	"time"

	"github.com/coder/websocket"
)

// runOrigin is the reconnect loop. Session >30s resets backoff; faster
// failures double it up to 15s.
func runOrigin(parent context.Context, c *Client) error {
	if c.cfg.Handler == nil {
		return fmt.Errorf("cfrelaytun: Config.Handler is required")
	}
	backoff := 500 * time.Millisecond
	for parent.Err() == nil {
		start := time.Now()
		err := runSession(parent, c)
		if time.Since(start) > 30*time.Second {
			backoff = 500 * time.Millisecond
		} else if backoff < 15*time.Second {
			backoff *= 2
		}
		if err != nil && c.cfg.Logger != nil {
			c.cfg.Logger("relay: %v (reconnecting in %s)", err, backoff)
		}
		select {
		case <-parent.Done():
			return parent.Err()
		case <-time.After(backoff):
		}
	}
	return parent.Err()
}

func runSession(ctx context.Context, c *Client) error {
	u, err := url.Parse(c.cfg.URL)
	if err != nil {
		return fmt.Errorf("parse url: %w", err)
	}
	q := u.Query()
	if c.cfg.Token != "" {
		q.Set("token", c.cfg.Token)
	}
	u.RawQuery = q.Encode()

	hdr := http.Header{"User-Agent": []string{"cfrelaytun/1.0"}}
	if c.cfg.Token != "" {
		hdr.Set("X-Origin-Token", c.cfg.Token)
	}

	conn, _, err := websocket.Dial(ctx, u.String(), &websocket.DialOptions{HTTPHeader: hdr})
	if err != nil {
		return fmt.Errorf("dial: %w", err)
	}
	defer conn.CloseNow()
	if c.cfg.Logger != nil {
		c.cfg.Logger("relay: connected to %s", c.cfg.URL)
	}

	a := &origin{
		client:  c,
		conn:    conn,
		streams: map[uint32]*originStream{},
		ws:      map[uint32]*wsStreamState{},
	}
	c.live = a
	defer func() { c.live = nil }()

	for _, fn := range c.onConnect {
		fn(ctx, a)
	}

	pingCtx, cancelPing := context.WithCancel(ctx)
	defer cancelPing()
	go a.pingLoop(pingCtx)

	return a.loop(ctx)
}

// origin holds the per-connection state for one relay session.
type origin struct {
	client *Client
	conn   *websocket.Conn

	writeMu sync.Mutex

	mu      sync.Mutex
	streams map[uint32]*originStream
	ws      map[uint32]*wsStreamState
}

type originStream struct {
	bodyR *io.PipeReader
	bodyW *io.PipeWriter
}

type wsStreamState struct {
	inbound chan wsInbound
	cancel  context.CancelFunc
}

type wsInbound struct {
	kind string // "text", "binary", "close"
	data []byte
}

// ─── send helpers ─────────────────────────────────────────────────────

func (a *origin) sendCtl(ctx context.Context, f ctlFrame) error {
	b, _ := json.Marshal(f)
	a.writeMu.Lock()
	defer a.writeMu.Unlock()
	return a.conn.Write(ctx, websocket.MessageText, b)
}

func (a *origin) sendBin(ctx context.Context, id uint32, data []byte) error {
	out := encodeBinary(id, data)
	a.writeMu.Lock()
	defer a.writeMu.Unlock()
	return a.conn.Write(ctx, websocket.MessageBinary, out)
}

// sendExtra serializes an arbitrary {t, ...payload} JSON object. Payload
// must marshal to a JSON object; its fields are merged with `t`.
func (a *origin) sendExtra(ctx context.Context, t string, payload any) error {
	var obj map[string]json.RawMessage
	if payload != nil {
		b, err := json.Marshal(payload)
		if err != nil {
			return err
		}
		if err := json.Unmarshal(b, &obj); err != nil {
			obj = nil
		}
	}
	if obj == nil {
		obj = map[string]json.RawMessage{}
	}
	tj, _ := json.Marshal(t)
	obj["t"] = tj
	b, _ := json.Marshal(obj)
	a.writeMu.Lock()
	defer a.writeMu.Unlock()
	return a.conn.Write(ctx, websocket.MessageText, b)
}

// SendCtl satisfies Session.
func (a *origin) SendCtl(ctx context.Context, t string, payload any) error {
	return a.sendExtra(ctx, t, payload)
}

// ─── ping ─────────────────────────────────────────────────────────────

func (a *origin) pingLoop(ctx context.Context) {
	t := time.NewTicker(25 * time.Second)
	defer t.Stop()
	for {
		select {
		case <-ctx.Done():
			return
		case <-t.C:
			pctx, cancel := context.WithTimeout(ctx, 10*time.Second)
			_ = a.conn.Ping(pctx)
			cancel()
		}
	}
}

// ─── main loop ────────────────────────────────────────────────────────

func (a *origin) loop(ctx context.Context) error {
	for {
		mt, data, err := a.conn.Read(ctx)
		if err != nil {
			return err
		}
		switch mt {
		case websocket.MessageText:
			a.onCtl(ctx, data)
		case websocket.MessageBinary:
			a.onBinary(ctx, data)
		}
	}
}

func (a *origin) onBinary(ctx context.Context, data []byte) {
	sid, body, ok := decodeBinary(data)
	if !ok {
		return
	}
	a.mu.Lock()
	s, ws := a.streams[sid], a.ws[sid]
	a.mu.Unlock()
	switch {
	case ws != nil:
		select {
		case ws.inbound <- wsInbound{kind: "binary", data: append([]byte(nil), body...)}:
		case <-ctx.Done():
		}
	case s != nil:
		_, _ = s.bodyW.Write(body)
	}
}

func (a *origin) onCtl(ctx context.Context, raw []byte) {
	t := peekType(raw)
	switch t {
	case "":
		return
	case "hello", "pong":
		return
	case "req":
		var f ctlFrame
		if err := json.Unmarshal(raw, &f); err != nil {
			return
		}
		pr, pw := io.Pipe()
		s := &originStream{bodyR: pr, bodyW: pw}
		a.mu.Lock()
		a.streams[f.ID] = s
		a.mu.Unlock()
		if !f.HasBody {
			_ = pw.Close()
		}
		go a.handleReq(ctx, &f, s)
	case "req-end":
		var f ctlFrame
		if err := json.Unmarshal(raw, &f); err != nil {
			return
		}
		if s := a.takeStream(f.ID, false); s != nil && s.bodyW != nil {
			_ = s.bodyW.Close()
		}
	case "cancel":
		var f ctlFrame
		if err := json.Unmarshal(raw, &f); err != nil {
			return
		}
		if s := a.takeStream(f.ID, true); s != nil && s.bodyW != nil {
			_ = s.bodyW.CloseWithError(io.ErrUnexpectedEOF)
		}
		a.mu.Lock()
		ws := a.ws[f.ID]
		delete(a.ws, f.ID)
		a.mu.Unlock()
		if ws != nil {
			select {
			case ws.inbound <- wsInbound{kind: "close"}:
			default:
			}
			ws.cancel()
		}
	case "ws-open":
		var f ctlFrame
		if err := json.Unmarshal(raw, &f); err != nil {
			return
		}
		go a.handleWSOpen(ctx, &f)
	case "ws-text":
		var f ctlFrame
		if err := json.Unmarshal(raw, &f); err != nil {
			return
		}
		a.mu.Lock()
		ws := a.ws[f.ID]
		a.mu.Unlock()
		if ws != nil {
			select {
			case ws.inbound <- wsInbound{kind: "text", data: []byte(f.Data)}:
			case <-ctx.Done():
			}
		}
	case "ws-close":
		var f ctlFrame
		if err := json.Unmarshal(raw, &f); err != nil {
			return
		}
		a.mu.Lock()
		ws := a.ws[f.ID]
		delete(a.ws, f.ID)
		a.mu.Unlock()
		if ws != nil {
			ws.cancel()
		}
	default:
		// Extension frame — hand to user.
		if h := a.client.extra[t]; h != nil {
			h(ctx, json.RawMessage(raw))
		}
	}
}

func (a *origin) takeStream(id uint32, remove bool) *originStream {
	a.mu.Lock()
	defer a.mu.Unlock()
	s := a.streams[id]
	if remove {
		delete(a.streams, id)
	}
	return s
}

// ─── HTTP tunnel ──────────────────────────────────────────────────────

func (a *origin) handleReq(ctx context.Context, f *ctlFrame, s *originStream) {
	defer func() {
		a.mu.Lock()
		delete(a.streams, f.ID)
		a.mu.Unlock()
	}()

	req, err := http.NewRequestWithContext(ctx, f.Method, f.Path, s.bodyR)
	if err != nil {
		_ = a.sendCtl(ctx, ctlFrame{T: "res-head", ID: f.ID, Status: 500})
		_ = a.sendCtl(ctx, ctlFrame{T: "res-end", ID: f.ID})
		return
	}
	for _, h := range f.Headers {
		if len(h) >= 2 {
			req.Header.Add(h[0], h[1])
		}
	}

	rw := &relayWriter{id: f.ID, origin: a, ctx: ctx, header: http.Header{}, status: 200}
	a.client.cfg.Handler.ServeHTTP(rw, req)
	rw.finish()
}

// relayWriter streams handler responses back as one head frame + N binary
// body frames + a res-end. Streaming flag set on first Flush so the relay
// forwards chunks immediately (SSE).
type relayWriter struct {
	id     uint32
	origin  *origin
	ctx    context.Context
	header http.Header
	status int
	sent   bool
	stream bool
	mu     sync.Mutex
}

func (w *relayWriter) Header() http.Header { return w.header }
func (w *relayWriter) WriteHeader(s int)   { w.status = s }
func (w *relayWriter) Write(b []byte) (int, error) {
	w.flushHead()
	if err := w.origin.sendBin(w.ctx, w.id, b); err != nil {
		return 0, err
	}
	return len(b), nil
}
func (w *relayWriter) Flush() { w.stream = true; w.flushHead() }
func (w *relayWriter) flushHead() {
	w.mu.Lock()
	defer w.mu.Unlock()
	if w.sent {
		return
	}
	w.sent = true
	hdrs := make([][]string, 0, len(w.header))
	for k, vs := range w.header {
		for _, v := range vs {
			hdrs = append(hdrs, []string{k, v})
		}
	}
	_ = w.origin.sendCtl(w.ctx, ctlFrame{
		T: "res-head", ID: w.id, Status: w.status,
		Headers: hdrs, Stream: w.stream,
	})
}
func (w *relayWriter) finish() {
	w.flushHead()
	_ = w.origin.sendCtl(w.ctx, ctlFrame{T: "res-end", ID: w.id})
}

// ─── WS tunnel ────────────────────────────────────────────────────────

func (a *origin) handleWSOpen(parent context.Context, f *ctlFrame) {
	if a.client.cfg.OnWSUpgrade == nil {
		_ = a.sendCtl(parent, ctlFrame{T: "ws-close", ID: f.ID, Code: 1011, Reason: "no ws handler"})
		return
	}

	dialCtx, cancel := context.WithCancel(parent)
	defer cancel()

	state := &wsStreamState{
		inbound: make(chan wsInbound, 32),
		cancel:  cancel,
	}
	a.mu.Lock()
	a.ws[f.ID] = state
	a.mu.Unlock()
	defer func() {
		a.mu.Lock()
		delete(a.ws, f.ID)
		a.mu.Unlock()
	}()

	req, err := http.NewRequestWithContext(dialCtx, "GET", f.Path, nil)
	if err != nil {
		_ = a.sendCtl(parent, ctlFrame{T: "ws-close", ID: f.ID, Code: 1011, Reason: err.Error()})
		return
	}
	for _, h := range f.Headers {
		if len(h) >= 2 {
			req.Header.Add(h[0], h[1])
		}
	}

	wc := &originWSConn{origin: a, id: f.ID, state: state}
	err = a.client.cfg.OnWSUpgrade(dialCtx, req, wc)
	if err != nil {
		_ = a.sendCtl(parent, ctlFrame{T: "ws-close", ID: f.ID, Code: 1011, Reason: err.Error()})
		return
	}
	_ = a.sendCtl(parent, ctlFrame{T: "ws-close", ID: f.ID, Code: 1000})
}

// originWSConn adapts the relay tunnel into a per-stream WS interface.
type originWSConn struct {
	origin *origin
	id    uint32
	state *wsStreamState
}

func (w *originWSConn) WriteText(ctx context.Context, s string) error {
	return w.origin.sendCtl(ctx, ctlFrame{T: "ws-text", ID: w.id, Data: s})
}
func (w *originWSConn) WriteBinary(ctx context.Context, b []byte) error {
	return w.origin.sendBin(ctx, w.id, b)
}
func (w *originWSConn) NextFrame(ctx context.Context) (string, []byte, error) {
	select {
	case <-ctx.Done():
		return "", nil, ctx.Err()
	case f, ok := <-w.state.inbound:
		if !ok {
			return "close", nil, nil
		}
		return f.kind, f.data, nil
	}
}
func (w *originWSConn) Close(code int, reason string) error {
	w.state.cancel()
	return w.origin.sendCtl(context.Background(), ctlFrame{
		T: "ws-close", ID: w.id, Code: code, Reason: reason,
	})
}

