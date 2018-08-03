package raft

type eventMsg struct {
	msgType string
	payload interface{}
}

type eventHandler func(interface{})

// simple, single-thread event loop
type eventLoop struct {
	ch chan *eventMsg

	stopCh chan int8

	handlers map[string]eventHandler
}

// make new event loop using given chan size
// (when pending messages' number exceeds this size, following `fireEvent` calls will block)
func makeEventLoop(chQueueSize int) *eventLoop {
	return &eventLoop{
		ch:       make(chan *eventMsg, chQueueSize),
		stopCh:   make(chan int8),
		handlers: make(map[string]eventHandler, 8),
	}
}

// Registering an event message handler against a message type.
// If a handler has been registered to this type, return the old one.
// This method is NOT concurrent-safe, never use it after calling `start`
func (el *eventLoop) registerHandler(msgType string, handler eventHandler) eventHandler {
	oldHandler := el.handlers[msgType]
	el.handlers[msgType] = handler

	return oldHandler
}

// Fire an event.
// If message type is unknown or this event loop has stopped, nothing happens.
func (el *eventLoop) fireEvent(msgType string, payload interface{}) (succeed bool) {
	if _, ok := el.handlers[msgType]; !ok {
		return false
	}

	select {
	case <-el.stopCh:
		return false
	case el.ch <- &eventMsg{msgType, payload}:
		return true
	}
}

// Transfer pending event messages to another event loop.
// `filter` is a callback to filter which message should be transferred.
// If `true` returned, the message would be transferred.
// Never use this method before event loop's stopping
func (el *eventLoop) transferEventTo(destination *eventLoop, filter func(msg *eventMsg) bool) {
	for {
		select {
		case msg := <-el.ch:
			if filter(msg) {
				destination.fireEvent(msg.msgType, msg.payload)
			}
		default:
			return
		}
	}
}

// Stop this event loop immediately, pending messages are left in queue,
// and no more message will be accepted.
func (el *eventLoop) stop() {
	select {
	case <-el.stopCh:
	default:
		close(el.stopCh)
	}
}

func (el *eventLoop) isStopped() bool {
	select {
	case <-el.stopCh:
		return true
	default:
		return false
	}
}

// Start the event loop in a new goroutine.
// It keeps running until `stop` is called.
func (el *eventLoop) start() {
	go func() {
		for {
			select {
			case <-el.stopCh:
				return

			case msg := <-el.ch:
				if el.isStopped() {
					return
				}

				el.handlers[msg.msgType](msg.payload)
			}
		}
	}()
}
