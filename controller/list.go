package main

type element struct {
	Value *replicationNode
	next  *element
	prev  *element
}

func (e *element) Next() *element {
	return e.next
}

func (e *element) Prev() *element {
	return e.prev
}

type list struct {
	front *element
	back  *element
	len   int
}

func newList() *list {
	return &list{len: 0}
}

func (l *list) Len() int {
	return l.len
}

func (l *list) Front() *element {
	return l.front
}

func (l *list) Back() *element {
	return l.back
}

func (l *list) PushBack(v *replicationNode) {
	el := &element{Value: v}
	if l.len == 0 {
		l.front = el
		l.back = el
	} else {
		back := l.back
		l.back.next = el
		l.back = el
		el.prev = back
	}
	l.len++
}

func (l *list) Remove(el *element) {
	if l.len == 0 {
		return
	}
	if l.len == 1 {
		l.front = nil
		l.back = nil
		l.len--
		return
	}
	if el == l.front {
		l.front = el.next
		l.len--
		return
	}
	if el == l.back {
		l.back = el.prev
		l.len--
		return
	}
	el.prev.next = el.next
	l.len--
}
