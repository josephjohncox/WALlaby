package postgres

import (
	"container/list"
)

type toastCache struct {
	max   int
	items map[string]*list.Element
	order *list.List
}

type toastCacheEntry struct {
	key string
	row map[string]any
}

func newToastCache(max int) *toastCache {
	if max <= 0 {
		return nil
	}
	return &toastCache{
		max:   max,
		items: make(map[string]*list.Element),
		order: list.New(),
	}
}

func (c *toastCache) Get(key string) (map[string]any, bool) {
	if c == nil || key == "" {
		return nil, false
	}
	if el, ok := c.items[key]; ok {
		c.order.MoveToFront(el)
		entry := el.Value.(*toastCacheEntry)
		return entry.row, true
	}
	return nil, false
}

func (c *toastCache) Put(key string, row map[string]any) {
	if c == nil || key == "" || row == nil {
		return
	}
	if el, ok := c.items[key]; ok {
		c.order.MoveToFront(el)
		entry := el.Value.(*toastCacheEntry)
		entry.row = cloneRow(row)
		return
	}
	entry := &toastCacheEntry{key: key, row: cloneRow(row)}
	el := c.order.PushFront(entry)
	c.items[key] = el
	for c.order.Len() > c.max {
		c.evictOldest()
	}
}

func (c *toastCache) Delete(key string) {
	if c == nil || key == "" {
		return
	}
	if el, ok := c.items[key]; ok {
		delete(c.items, key)
		c.order.Remove(el)
	}
}

func (c *toastCache) evictOldest() {
	el := c.order.Back()
	if el == nil {
		return
	}
	entry := el.Value.(*toastCacheEntry)
	delete(c.items, entry.key)
	c.order.Remove(el)
}

func cloneRow(row map[string]any) map[string]any {
	if row == nil {
		return nil
	}
	out := make(map[string]any, len(row))
	for key, val := range row {
		out[key] = val
	}
	return out
}
