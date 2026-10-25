package main

import "sync"

type Room struct {
	id      string
	clients map[*Client]struct{}
	mu      sync.RWMutex
}

func newRoom(id string) *Room {
	return &Room{
		id:      id,
		clients: make(map[*Client]struct{}),
	}
}

func (r *Room) addClient(c *Client) {
	r.mu.Lock()
	r.clients[c] = struct{}{}
	r.mu.Unlock()
}

func (r *Room) removeClient(c *Client) bool {
	r.mu.Lock()
	delete(r.clients, c)
	empty := len(r.clients) == 0
	r.mu.Unlock()
	return empty
}

func (r *Room) onlineCount() int {
	r.mu.RLock()
	n := len(r.clients)
	r.mu.RUnlock()
	return n
}

// broadcast sends msg to every client in the room.
// Slow clients with full send buffers are disconnected to avoid blocking.
func (r *Room) broadcast(msg []byte) {
	r.mu.RLock()
	defer r.mu.RUnlock()

	for c := range r.clients {
		select {
		case c.send <- msg:
		default:
			// slow client - drop it so it doesn't block everyone else
			go c.close()
		}
	}
}

type Hub struct {
	rooms map[string]*Room
	mu    sync.RWMutex
}

func newHub() *Hub {
	return &Hub{rooms: make(map[string]*Room)}
}

func (h *Hub) registerClient(roomID string, c *Client) {
	h.mu.Lock()
	defer h.mu.Unlock()

	r, ok := h.rooms[roomID]
	if !ok {
		r = newRoom(roomID)
		h.rooms[roomID] = r
	}
	r.addClient(c)
}

func (h *Hub) getRoom(id string) *Room {
	h.mu.RLock()
	defer h.mu.RUnlock()
	return h.rooms[id]
}

func (h *Hub) removeClientFromRoom(roomID string, c *Client) {
	h.mu.Lock()
	defer h.mu.Unlock()

	r, ok := h.rooms[roomID]
	if !ok {
		return
	}
	if r.removeClient(c) {
		delete(h.rooms, roomID)
	}
}