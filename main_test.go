package main

import (
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"

	"github.com/gorilla/websocket"
)

func setupTestServer(t *testing.T) (*httptest.Server, *Hub) {
	t.Helper()
	hub := newHub()
	mux := http.NewServeMux()
	mux.HandleFunc("GET /health", handleHealth)
	mux.HandleFunc("GET /api/v1/rooms/{roomID}", handleRoomInfo(hub))
	mux.HandleFunc("GET /ws/rooms/{roomID}", handleWS(hub))
	return httptest.NewServer(mux), hub
}

func dialWS(t *testing.T, srv *httptest.Server, roomID string) *websocket.Conn {
	t.Helper()
	url := "ws" + strings.TrimPrefix(srv.URL, "http") + "/ws/rooms/" + roomID
	conn, _, err := websocket.DefaultDialer.Dial(url, nil)
	if err != nil {
		t.Fatalf("dial %s: %v", roomID, err)
	}
	return conn
}

func sendChat(t *testing.T, conn *websocket.Conn, msg string) {
	t.Helper()
	payload, _ := json.Marshal(map[string]string{"type": "chat.message", "message": msg})
	if err := conn.WriteMessage(websocket.TextMessage, payload); err != nil {
		t.Fatalf("write: %v", err)
	}
}

func readMsg(t *testing.T, conn *websocket.Conn) map[string]interface{} {
	t.Helper()
	conn.SetReadDeadline(time.Now().Add(3 * time.Second))
	_, raw, err := conn.ReadMessage()
	if err != nil {
		t.Fatalf("read: %v", err)
	}
	var m map[string]interface{}
	json.Unmarshal(raw, &m)
	return m
}

func TestSendReceiveMessage(t *testing.T) {
	srv, _ := setupTestServer(t)
	defer srv.Close()

	c1 := dialWS(t, srv, "room1")
	defer c1.Close()
	c2 := dialWS(t, srv, "room1")
	defer c2.Close()

	time.Sleep(50 * time.Millisecond)
	sendChat(t, c1, "hello from c1")

	m1 := readMsg(t, c1)
	m2 := readMsg(t, c2)

	if m1["message"] != "hello from c1" || m2["message"] != "hello from c1" {
		t.Fatalf("unexpected messages: %v / %v", m1, m2)
	}
	if m1["room_id"] != "room1" || m2["room_id"] != "room1" {
		t.Fatalf("wrong room_id in broadcast")
	}
	if m1["id"] == nil || m1["user_id"] == nil || m1["timestamp"] == nil {
		t.Fatalf("missing fields in outgoing message")
	}
}

func TestRoomIsolation(t *testing.T) {
	srv, _ := setupTestServer(t)
	defer srv.Close()

	cA := dialWS(t, srv, "roomA")
	defer cA.Close()
	cB := dialWS(t, srv, "roomB")
	defer cB.Close()

	time.Sleep(50 * time.Millisecond)
	sendChat(t, cA, "message for roomA")

	mA := readMsg(t, cA)
	if mA["message"] != "message for roomA" {
		t.Fatalf("roomA user didn't get their message")
	}

	cB.SetReadDeadline(time.Now().Add(300 * time.Millisecond))
	_, _, err := cB.ReadMessage()
	if err == nil {
		t.Fatalf("roomB user received a message from roomA - isolation broken")
	}
}

func TestMessageValidation(t *testing.T) {
	srv, _ := setupTestServer(t)
	defer srv.Close()

	c := dialWS(t, srv, "room_val")
	defer c.Close()
	time.Sleep(50 * time.Millisecond)

	sendChat(t, c, "   ")
	m := readMsg(t, c)
	if m["type"] != "error" {
		t.Fatalf("expected error for empty message, got %v", m)
	}

	longMsg := strings.Repeat("سلام", 200)
	sendChat(t, c, longMsg)
	m = readMsg(t, c)
	if m["type"] != "error" {
		t.Fatalf("expected error for long message, got %v", m)
	}
	if !strings.Contains(m["error"].(string), "500") {
		t.Fatalf("error should mention 500 char limit: %v", m["error"])
	}
}

func TestDisconnectDecrementsCount(t *testing.T) {
	srv, hub := setupTestServer(t)
	defer srv.Close()

	c1 := dialWS(t, srv, "room_dc")
	c2 := dialWS(t, srv, "room_dc")
	time.Sleep(100 * time.Millisecond)

	room := hub.getRoom("room_dc")
	if room == nil {
		t.Fatal("room should exist")
	}
	if n := room.onlineCount(); n != 2 {
		t.Fatalf("expected 2 online, got %d", n)
	}

	c1.Close()
	time.Sleep(200 * time.Millisecond)

	if n := room.onlineCount(); n != 1 {
		t.Fatalf("expected 1 online after disconnect, got %d", n)
	}

	c2.Close()
	time.Sleep(200 * time.Millisecond)

	if r := hub.getRoom("room_dc"); r != nil {
		t.Fatalf("expected room to be removed, but it still exists with %d clients", r.onlineCount())
	}
}

func TestRateLimiting(t *testing.T) {
	srv, _ := setupTestServer(t)
	defer srv.Close()

	c := dialWS(t, srv, "room_rl")
	defer c.Close()
	time.Sleep(50 * time.Millisecond)

	for i := 0; i < 10; i++ {
		sendChat(t, c, "flood")
	}

	gotError := false
	for i := 0; i < 10; i++ {
		c.SetReadDeadline(time.Now().Add(2 * time.Second))
		_, raw, err := c.ReadMessage()
		if err != nil {
			break
		}
		var m map[string]interface{}
		json.Unmarshal(raw, &m)
		if m["type"] == "error" && strings.Contains(m["error"].(string), "rate limit") {
			gotError = true
			break
		}
	}
	if !gotError {
		t.Fatal("expected rate limit error after flooding")
	}
}
