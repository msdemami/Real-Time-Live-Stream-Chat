package main

import (
	"crypto/rand"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"strings"
	"sync"
	"time"
	"unicode/utf8"

	"github.com/gorilla/websocket"
	"golang.org/x/time/rate"
)

const (
	maxMessageSize = 4096
	maxMsgRunes    = 500
	writeWait      = 10 * time.Second
	pongWait       = 60 * time.Second
	pingInterval   = 54 * time.Second
	sendBufSize    = 64
	rateLimit      = 5
	rateBurst      = 5
)

type IncomingMsg struct {
	Type    string `json:"type"`
	Message string `json:"message"`
}

type OutgoingMsg struct {
	ID        string `json:"id"`
	Type      string `json:"type"`
	RoomID    string `json:"room_id"`
	UserID    string `json:"user_id"`
	Message   string `json:"message"`
	Timestamp string `json:"timestamp"`
}

type ErrorMsg struct {
	Type  string `json:"type"`
	Error string `json:"error"`
}

type Client struct {
	hub     *Hub
	conn    *websocket.Conn
	send    chan []byte
	done    chan struct{}
	roomID  string
	userID  string
	limiter *rate.Limiter
	once    sync.Once
}

func newClient(hub *Hub, conn *websocket.Conn, roomID string) *Client {
	b := make([]byte, 4)
	rand.Read(b)
	return &Client{
		hub:     hub,
		conn:    conn,
		send:    make(chan []byte, sendBufSize),
		done:    make(chan struct{}),
		roomID:  roomID,
		userID:  "user_" + hex.EncodeToString(b),
		limiter: rate.NewLimiter(rateLimit, rateBurst),
	}
}

func (c *Client) close() {
	c.once.Do(func() {
		close(c.done)
		c.hub.removeClientFromRoom(c.roomID, c)
		c.conn.Close()
	})
}

func (c *Client) sendError(msg string) {
	data, _ := json.Marshal(ErrorMsg{Type: "error", Error: msg})
	select {
	case c.send <- data:
	default:
	}
}

func (c *Client) readPump() {
	defer c.close()

	c.conn.SetReadLimit(maxMessageSize)
	c.conn.SetReadDeadline(time.Now().Add(pongWait))
	c.conn.SetPongHandler(func(string) error {
		c.conn.SetReadDeadline(time.Now().Add(pongWait))
		return nil
	})

	for {
		_, raw, err := c.conn.ReadMessage()
		if err != nil {
			return
		}

		if !c.limiter.Allow() {
			c.sendError("rate limit exceeded")
			continue
		}

		var in IncomingMsg
		if err := json.Unmarshal(raw, &in); err != nil {
			c.sendError("invalid JSON")
			continue
		}

		if in.Type != "chat.message" {
			c.sendError("unsupported message type")
			continue
		}

		trimmed := strings.TrimSpace(in.Message)
		if trimmed == "" {
			c.sendError("message must not be empty")
			continue
		}

		if utf8.RuneCountInString(trimmed) > maxMsgRunes {
			c.sendError(fmt.Sprintf("message exceeds %d characters", maxMsgRunes))
			continue
		}

		msgID := generateMsgID()
		out := OutgoingMsg{
			ID:        msgID,
			Type:      "chat.message",
			RoomID:    c.roomID,
			UserID:    c.userID,
			Message:   trimmed,
			Timestamp: time.Now().UTC().Format(time.RFC3339),
		}
		data, _ := json.Marshal(out)

		room := c.hub.getRoom(c.roomID)
		if room != nil {
			room.broadcast(data)
		}
	}
}

func (c *Client) writePump() {
	ticker := time.NewTicker(pingInterval)
	defer func() {
		ticker.Stop()
		c.close()
	}()

	for {
		select {
		case <-c.done:
			return
		case msg := <-c.send:
			c.conn.SetWriteDeadline(time.Now().Add(writeWait))
			if err := c.conn.WriteMessage(websocket.TextMessage, msg); err != nil {
				return
			}
		case <-ticker.C:
			c.conn.SetWriteDeadline(time.Now().Add(writeWait))
			if err := c.conn.WriteMessage(websocket.PingMessage, nil); err != nil {
				return
			}
		}
	}
}

func generateMsgID() string {
	b := make([]byte, 8)
	rand.Read(b)
	return hex.EncodeToString(b)
}