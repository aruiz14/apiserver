package subscribe

import (
	"context"
	"crypto/md5"
	"encoding/json"
	"fmt"
	"os"
	"sync"
	"time"

	"github.com/gorilla/websocket"
	"github.com/rancher/apiserver/pkg/types"
	"github.com/sirupsen/logrus"
	"go.opentelemetry.io/otel"
	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/codes"
	"go.opentelemetry.io/otel/trace"
	"go.opentelemetry.io/otel/trace/noop"
	"k8s.io/apimachinery/pkg/watch"
)

type WatchSession struct {
	sync.Mutex

	apiOp    *types.APIRequest
	getter   SchemasGetter
	watchers map[string]func()
	wg       sync.WaitGroup
	ctx      context.Context
	cancel   func()
}

func (s *WatchSession) stop(sub Subscribe, resp chan<- types.APIEvent) {
	s.Lock()
	defer s.Unlock()
	if cancel, ok := s.watchers[sub.key()]; ok {
		cancel()
		resp <- types.APIEvent{
			Name:         "resource.stop",
			ResourceType: sub.ResourceType,
			Namespace:    sub.Namespace,
			ID:           sub.ID,
			Selector:     sub.Selector,
			Mode:         string(sub.Mode),
		}
	}
	delete(s.watchers, sub.key())
}

func (s *WatchSession) add(sub Subscribe, resp chan<- types.APIEvent) {
	s.Lock()
	defer s.Unlock()

	ctx, cancel := context.WithCancel(s.ctx)
	s.watchers[sub.key()] = cancel

	s.wg.Add(1)
	go func() {
		defer s.wg.Done()
		defer s.stop(sub, resp)

		if err := s.stream(ctx, sub, resp); err != nil {
			sendErr(resp, err, sub)
		}
	}()
}

func (s *WatchSession) stream(ctx context.Context, sub Subscribe, result chan<- types.APIEvent) error {
	schemas := s.getter(s.apiOp)
	schema := schemas.LookupSchema(sub.ResourceType)
	if schema == nil {
		return fmt.Errorf("failed to find schema %s", sub.ResourceType)
	} else if schema.Store == nil {
		return fmt.Errorf("schema %s does not support watching", sub.ResourceType)
	}

	if err := s.apiOp.AccessControl.CanWatch(s.apiOp, schema); err != nil {
		return err
	}

	apiOp := s.apiOp.Clone().WithContext(ctx)
	apiOp.Namespace = sub.Namespace
	apiOp.Schemas = schemas
	c, err := schema.Store.Watch(apiOp, schema, types.WatchRequest{
		Revision: sub.ResourceVersion,
		ID:       sub.ID,
		Selector: sub.Selector,
	})
	if err != nil {
		return err
	}

	result <- types.APIEvent{
		Name:         "resource.start",
		ResourceType: sub.ResourceType,
		Namespace:    sub.Namespace,
		ID:           sub.ID,
		Selector:     sub.Selector,
		Mode:         string(sub.Mode),
	}

	if sub.Mode == SubscriptionModeNotification {
		debounceRate := time.Duration(sub.DebounceMs) * time.Millisecond
		if debounceRate == 0 {
			debounceRate = 5000 * time.Millisecond
		}

		debounce := newDebouncer(debounceRate, c)
		go debounce.Run(ctx)
		c = debounce.NotificationsChan()
	}

	if c == nil {
		<-s.apiOp.Context().Done()
	} else {
		for event := range c {
			if event.Error != nil {
				sendErr(result, event.Error, sub)
				continue
			}
			var eventType string
			rv := event.Revision
			tracer := noop.NewTracerProvider().Tracer("")
			if sub.ResourceType == "configmaps" {
				switch event.Name {
				case types.CreateAPIEvent:
					eventType = string(watch.Added)
				case types.ChangeAPIEvent:
					eventType = string(watch.Modified)
				case types.RemoveAPIEvent:
					eventType = string(watch.Deleted)
				}
				if eventType != "" {
					tracer = otel.Tracer("", trace.WithInstrumentationAttributes(
						attribute.String("event.type", eventType),
						attribute.String("object.resourceVersion", rv),
						attribute.String("object.key", event.Object.ID),
					))
				}
			}

			ctx := rootContextForResourceVersion(ctx, rv)
			_, span := tracer.Start(ctx, "websocket.send")

			event.ID = sub.ID
			event.Selector = sub.Selector
			event.ResourceType = sub.ResourceType
			event.Namespace = sub.Namespace
			event.Mode = string(sub.Mode)

			select {
			case result <- event:
			// Give enough time for consumer to handle events, for
			// example when many objects are in the c channel
			case <-time.After(10 * time.Millisecond):
				span.SetStatus(codes.Error, "timeout delivering websocket message")
				span.End()
				logrus.Warnf("Event took >10ms to be sent via websocket, closing stream!")
				// handle slow consumer
				go func() {
					for range c {
						// continue to drain until close
					}
				}()
				return nil
			}
			span.AddEvent("Websocket message sent!")
			span.End()
		}
	}

	return nil
}

func rootContextForResourceVersion(ctx context.Context, rv string) context.Context {
	return trace.ContextWithRemoteSpanContext(ctx, trace.NewSpanContext(trace.SpanContextConfig{
		TraceID:    traceIdFromResourceVersion(rv),
		SpanID:     spanIdFromString(os.Getenv("HOSTNAME"), "watcher"), // TODO: SHOULD BE STORE, BUT NEEDS REFACTORING
		TraceFlags: traceFlagsForResourveVersion(rv),
	}))
}

func NewWatchSession(apiOp *types.APIRequest, getter SchemasGetter) *WatchSession {
	ws := &WatchSession{
		apiOp:    apiOp,
		getter:   getter,
		watchers: map[string]func(){},
	}

	ws.ctx, ws.cancel = context.WithCancel(apiOp.Request.Context())
	return ws
}

func (s *WatchSession) Watch(conn *websocket.Conn) <-chan types.APIEvent {
	result := make(chan types.APIEvent, 100)
	go func() {
		defer close(result)

		if err := s.watch(conn, result); err != nil {
			sendErr(result, err, Subscribe{})
		}
	}()
	return result
}

func (s *WatchSession) Close() {
	s.cancel()
	s.wg.Wait()
}

func (s *WatchSession) watch(conn *websocket.Conn, resp chan types.APIEvent) error {
	defer s.wg.Wait()
	defer s.cancel()

	for {
		_, r, err := conn.NextReader()
		if err != nil {
			return err
		}

		var sub Subscribe

		if err := json.NewDecoder(r).Decode(&sub); err != nil {
			sendErr(resp, err, Subscribe{})
			continue
		}

		if sub.Stop {
			s.stop(sub, resp)
		} else {
			s.Lock()
			_, ok := s.watchers[sub.key()]
			s.Unlock()
			if !ok {
				s.add(sub, resp)
			}
		}
	}
}

func sendErr(resp chan<- types.APIEvent, err error, sub Subscribe) {
	resp <- types.APIEvent{
		ResourceType: sub.ResourceType,
		Namespace:    sub.Namespace,
		ID:           sub.ID,
		Selector:     sub.Selector,
		Mode:         string(sub.Mode),
		Error:        err,
	}
}

func traceIdFromResourceVersion(s string) trace.TraceID {
	var tid trace.TraceID
	hash := md5.Sum([]byte(s))
	copy(tid[:], hash[:])
	return tid
}

func spanIdFromString(values ...string) trace.SpanID {
	sum := md5.New()
	for _, s := range values {
		sum.Write([]byte(s))
	}
	hash := sum.Sum(nil)

	var sid trace.SpanID
	copy(sid[:], hash)
	return sid
}

func traceFlagsForResourveVersion(resourceVersion string) trace.TraceFlags {
	var flags trace.TraceFlags
	// sample odd numbers
	flags = flags.WithSampled(resourceVersion[len(resourceVersion)-1]%2 == 1)
	return flags
}
