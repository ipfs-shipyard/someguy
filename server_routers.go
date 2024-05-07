package main

import (
	"context"
	"encoding/json"
	"errors"
	"io"
	"math"
	"path/filepath"
	"reflect"
	"sync"
	"time"

	"github.com/ipfs/boxo/ipns"
	"github.com/ipfs/boxo/routing/http/client"
	"github.com/ipfs/boxo/routing/http/server"
	"github.com/ipfs/boxo/routing/http/types"
	"github.com/ipfs/boxo/routing/http/types/iter"
	"github.com/ipfs/go-cid"
	"github.com/ipfs/go-datastore"
	leveldb "github.com/ipfs/go-ds-leveldb"
	"github.com/libp2p/go-libp2p/core/peer"
	"github.com/libp2p/go-libp2p/core/routing"
	manet "github.com/multiformats/go-multiaddr/net"
	"github.com/samber/lo"
)

type router interface {
	providersRouter
	peersRouter
	ipnsRouter
	io.Closer
}

type providersRouter interface {
	FindProviders(ctx context.Context, cid cid.Cid, limit int) (iter.ResultIter[types.Record], error)
	Provide(ctx context.Context, req *types.AnnouncementRecord) (time.Duration, error)
	io.Closer
}

type peersRouter interface {
	FindPeers(ctx context.Context, pid peer.ID, limit int) (iter.ResultIter[*types.PeerRecord], error)
	ProvidePeer(ctx context.Context, req *types.AnnouncementRecord) (time.Duration, error)
	io.Closer
}

type ipnsRouter interface {
	GetIPNS(ctx context.Context, name ipns.Name) (*ipns.Record, error)
	PutIPNS(ctx context.Context, name ipns.Name, record *ipns.Record) error
	io.Closer
}

var _ server.ContentRouter = composableRouter{}

type composableRouter struct {
	providers providersRouter
	peers     peersRouter
	ipns      ipnsRouter
}

func (r composableRouter) FindProviders(ctx context.Context, key cid.Cid, limit int) (iter.ResultIter[types.Record], error) {
	if r.providers == nil {
		return iter.ToResultIter(iter.FromSlice([]types.Record{})), nil
	}
	return r.providers.FindProviders(ctx, key, limit)
}

func (r composableRouter) Provide(ctx context.Context, req *types.AnnouncementRecord) (time.Duration, error) {
	if r.providers == nil {
		return 0, nil
	}
	return r.providers.Provide(ctx, req)
}

func (r composableRouter) FindPeers(ctx context.Context, pid peer.ID, limit int) (iter.ResultIter[*types.PeerRecord], error) {
	if r.peers == nil {
		return iter.ToResultIter(iter.FromSlice([]*types.PeerRecord{})), nil
	}
	return r.peers.FindPeers(ctx, pid, limit)
}

func (r composableRouter) ProvidePeer(ctx context.Context, req *types.AnnouncementRecord) (time.Duration, error) {
	if r.peers == nil {
		return 0, nil
	}
	return r.peers.ProvidePeer(ctx, req)
}

func (r composableRouter) GetIPNS(ctx context.Context, name ipns.Name) (*ipns.Record, error) {
	if r.ipns == nil {
		return nil, routing.ErrNotFound
	}
	return r.ipns.GetIPNS(ctx, name)
}

func (r composableRouter) PutIPNS(ctx context.Context, name ipns.Name, record *ipns.Record) error {
	if r.ipns == nil {
		return nil
	}
	return r.ipns.PutIPNS(ctx, name, record)
}

func (r composableRouter) Close() error {
	var err error

	if r.providers != nil {
		err = errors.Join(err, r.providers.Close())
	}

	if r.peers != nil {
		err = errors.Join(err, r.peers.Close())
	}

	if r.ipns != nil {
		err = errors.Join(err, r.ipns.Close())
	}

	return err
}

var _ server.ContentRouter = parallelRouter{}

type parallelRouter struct {
	routers []router
}

func (r parallelRouter) FindProviders(ctx context.Context, key cid.Cid, limit int) (iter.ResultIter[types.Record], error) {
	return find(ctx, r.routers, func(ri router) (iter.ResultIter[types.Record], error) {
		return ri.FindProviders(ctx, key, limit)
	})
}

func (r parallelRouter) FindPeers(ctx context.Context, pid peer.ID, limit int) (iter.ResultIter[*types.PeerRecord], error) {
	return find(ctx, r.routers, func(ri router) (iter.ResultIter[*types.PeerRecord], error) {
		return ri.FindPeers(ctx, pid, limit)
	})
}

func find[T any](ctx context.Context, routers []router, call func(router) (iter.ResultIter[T], error)) (iter.ResultIter[T], error) {
	switch len(routers) {
	case 0:
		return iter.ToResultIter(iter.FromSlice([]T{})), nil
	case 1:
		return call(routers[0])
	}

	its := make([]iter.ResultIter[T], 0, len(routers))
	var err error
	for _, ri := range routers {
		it, itErr := call(ri)

		if itErr != nil {
			logger.Warnf("error from router: %w", itErr)
			err = errors.Join(err, itErr)
		} else {
			its = append(its, it)
		}
	}

	// If all iterators failed to be created, then return the error.
	if len(its) == 0 {
		logger.Warnf("failed to create all iterators: %w", err)
		return nil, err
	} else if err != nil {
		logger.Warnf("failed to create some iterators: %w", err)
	}

	// Otherwise return manyIter with remaining iterators.
	return newManyIter(ctx, its), nil
}

type manyIter[T any] struct {
	ctx    context.Context
	cancel context.CancelFunc
	wg     sync.WaitGroup
	its    []iter.ResultIter[T]
	ch     chan iter.Result[T]
	val    iter.Result[T]
	done   bool
}

func newManyIter[T any](ctx context.Context, its []iter.ResultIter[T]) *manyIter[T] {
	ctx, cancel := context.WithCancel(ctx)

	mi := &manyIter[T]{
		ctx:    ctx,
		cancel: cancel,
		its:    its,
		ch:     make(chan iter.Result[T]),
	}

	for _, it := range its {
		mi.wg.Add(1)
		go func(it iter.ResultIter[T]) {
			defer mi.wg.Done()
			for it.Next() {
				select {
				case mi.ch <- it.Val():
				case <-ctx.Done():
					return
				}
			}
		}(it)
	}

	go func() {
		mi.wg.Wait()
		close(mi.ch)
	}()

	return mi
}

func (mi *manyIter[T]) Next() bool {
	if mi.done {
		return false
	}

	select {
	case val, ok := <-mi.ch:
		if ok {
			mi.val = val
		} else {
			mi.done = true
		}
	case <-mi.ctx.Done():
		mi.done = true
	}

	return !mi.done
}

func (mi *manyIter[T]) Val() iter.Result[T] {
	return mi.val
}

func (mi *manyIter[T]) Close() error {
	mi.done = true
	mi.cancel()
	mi.wg.Wait()
	var err error
	for _, it := range mi.its {
		err = errors.Join(err, it.Close())
	}
	if err != nil {
		logger.Warnf("errors on closing iterators: %w", err)
	}
	return err
}

func (r parallelRouter) Provide(ctx context.Context, req *types.AnnouncementRecord) (time.Duration, error) {
	return provide(ctx, r.routers, func(ctx context.Context, r router) (time.Duration, error) {
		return r.Provide(ctx, req)
	})
}

func (r parallelRouter) ProvidePeer(ctx context.Context, req *types.AnnouncementRecord) (time.Duration, error) {
	return provide(ctx, r.routers, func(ctx context.Context, r router) (time.Duration, error) {
		return r.ProvidePeer(ctx, req)
	})
}

func provide(ctx context.Context, routers []router, call func(context.Context, router) (time.Duration, error)) (time.Duration, error) {
	switch len(routers) {
	case 0:
		return 0, nil
	case 1:
		return call(ctx, routers[0])
	}

	ctx, cancel := context.WithCancel(ctx)
	defer cancel()

	var wg sync.WaitGroup
	resultsTTL := make([]time.Duration, len(routers))
	resultsErr := make([]error, len(routers))
	wg.Add(len(routers))
	for i, ri := range routers {
		go func(ri router, i int) {
			resultsTTL[i], resultsErr[i] = call(ctx, ri)
			wg.Done()
		}(ri, i)
	}
	wg.Wait()

	var err error
	for _, e := range resultsErr {
		err = errors.Join(err, e)
	}

	// Choose lowest TTL to return.
	var ttl time.Duration = math.MaxInt64
	for _, t := range resultsTTL {
		if t < ttl {
			ttl = t
		}
	}

	return ttl, err
}

func (r parallelRouter) GetIPNS(ctx context.Context, name ipns.Name) (*ipns.Record, error) {
	switch len(r.routers) {
	case 0:
		return nil, routing.ErrNotFound
	case 1:
		return r.routers[0].GetIPNS(ctx, name)
	}

	ctx, cancel := context.WithCancel(ctx)
	defer cancel()

	results := make(chan struct {
		val *ipns.Record
		err error
	})
	for _, ri := range r.routers {
		go func(ri router) {
			value, err := ri.GetIPNS(ctx, name)
			select {
			case results <- struct {
				val *ipns.Record
				err error
			}{
				val: value,
				err: err,
			}:
			case <-ctx.Done():
			}
		}(ri)
	}

	var errs error

	for range r.routers {
		select {
		case res := <-results:
			switch res.err {
			case nil:
				return res.val, nil
			case routing.ErrNotFound, routing.ErrNotSupported:
				continue
			}
			// If the context has expired, just return that error
			// and ignore the other errors.
			if ctx.Err() != nil {
				return nil, ctx.Err()
			}

			errs = errors.Join(errs, res.err)
		case <-ctx.Done():
			return nil, ctx.Err()
		}
	}

	if errs == nil {
		return nil, routing.ErrNotFound
	}

	return nil, errs
}

func (r parallelRouter) PutIPNS(ctx context.Context, name ipns.Name, record *ipns.Record) error {
	switch len(r.routers) {
	case 0:
		return nil
	case 1:
		return r.routers[0].PutIPNS(ctx, name, record)
	}

	ctx, cancel := context.WithCancel(ctx)
	defer cancel()

	var wg sync.WaitGroup
	results := make([]error, len(r.routers))
	wg.Add(len(r.routers))
	for i, ri := range r.routers {
		go func(ri router, i int) {
			results[i] = ri.PutIPNS(ctx, name, record)
			wg.Done()
		}(ri, i)
	}
	wg.Wait()

	var errs error
	for _, err := range results {
		errs = errors.Join(errs, err)
	}
	return errs
}

func (r parallelRouter) Close() error {
	var err error

	for _, r := range r.routers {
		err = errors.Join(err, r.Close())
	}

	return err
}

var _ router = libp2pRouter{}

type libp2pRouter struct {
	putEnabled bool
	routing    routing.Routing
}

func (r libp2pRouter) FindProviders(ctx context.Context, key cid.Cid, limit int) (iter.ResultIter[types.Record], error) {
	ctx, cancel := context.WithCancel(ctx)
	ch := r.routing.FindProvidersAsync(ctx, key, limit)
	return iter.ToResultIter(&peerChanIter{
		ch:     ch,
		cancel: cancel,
	}), nil
}

func (r libp2pRouter) Provide(ctx context.Context, req *types.AnnouncementRecord) (time.Duration, error) {
	// NOTE: the libp2p router cannot provide records further into the DHT.
	return 0, routing.ErrNotSupported
}

func (r libp2pRouter) FindPeers(ctx context.Context, pid peer.ID, limit int) (iter.ResultIter[*types.PeerRecord], error) {
	ctx, cancel := context.WithCancel(ctx)
	defer cancel()

	addr, err := r.routing.FindPeer(ctx, pid)
	if err != nil {
		return nil, err
	}

	rec := &types.PeerRecord{
		Schema: types.SchemaPeer,
		ID:     &addr.ID,
	}

	for _, addr := range addr.Addrs {
		rec.Addrs = append(rec.Addrs, types.Multiaddr{Multiaddr: addr})
	}

	return iter.ToResultIter(iter.FromSlice([]*types.PeerRecord{rec})), nil
}

func (r libp2pRouter) ProvidePeer(ctx context.Context, req *types.AnnouncementRecord) (time.Duration, error) {
	// NOTE: the libp2p router cannot provide peers further into the DHT.
	return 0, routing.ErrNotSupported
}

func (r libp2pRouter) GetIPNS(ctx context.Context, name ipns.Name) (*ipns.Record, error) {
	ctx, cancel := context.WithCancel(ctx)
	defer cancel()

	raw, err := r.routing.GetValue(ctx, string(name.RoutingKey()))
	if err != nil {
		return nil, err
	}

	return ipns.UnmarshalRecord(raw)
}

func (r libp2pRouter) PutIPNS(ctx context.Context, name ipns.Name, record *ipns.Record) error {
	if !r.putEnabled {
		return routing.ErrNotSupported
	}

	ctx, cancel := context.WithCancel(ctx)
	defer cancel()

	raw, err := ipns.MarshalRecord(record)
	if err != nil {
		return err
	}

	return r.routing.PutValue(ctx, string(name.RoutingKey()), raw)
}

func (r libp2pRouter) Close() error {
	return nil
}

type peerChanIter struct {
	ch     <-chan peer.AddrInfo
	cancel context.CancelFunc
	next   *peer.AddrInfo
}

func (it *peerChanIter) Next() bool {
	addr, ok := <-it.ch
	if ok {
		it.next = &addr
		return true
	}
	it.next = nil
	return false
}

func (it *peerChanIter) Val() types.Record {
	if it.next == nil {
		return nil
	}

	rec := &types.PeerRecord{
		Schema: types.SchemaPeer,
		ID:     &it.next.ID,
	}

	for _, addr := range it.next.Addrs {
		rec.Addrs = append(rec.Addrs, types.Multiaddr{Multiaddr: addr})
	}

	return rec
}

func (it *peerChanIter) Close() error {
	it.cancel()
	return nil
}

var _ router = clientRouter{}

type clientRouter struct {
	putEnabled bool
	client     *client.Client
}

func (r clientRouter) FindProviders(ctx context.Context, cid cid.Cid, limit int) (iter.ResultIter[types.Record], error) {
	return r.client.FindProviders(ctx, cid)
}

func (r clientRouter) Provide(ctx context.Context, req *types.AnnouncementRecord) (time.Duration, error) {
	if !r.putEnabled {
		return 0, routing.ErrNotSupported
	}

	return r.provide(func() (iter.ResultIter[*types.AnnouncementResponseRecord], error) {
		return r.client.ProvideRecords(ctx, req)
	})
}

func (r clientRouter) FindPeers(ctx context.Context, pid peer.ID, limit int) (iter.ResultIter[*types.PeerRecord], error) {
	return r.client.FindPeers(ctx, pid)
}

func (r clientRouter) ProvidePeer(ctx context.Context, req *types.AnnouncementRecord) (time.Duration, error) {
	if !r.putEnabled {
		return 0, routing.ErrNotSupported
	}

	return r.provide(func() (iter.ResultIter[*types.AnnouncementResponseRecord], error) {
		return r.client.ProvidePeerRecords(ctx, req)
	})
}

func (r clientRouter) GetIPNS(ctx context.Context, name ipns.Name) (*ipns.Record, error) {
	return r.client.GetIPNS(ctx, name)
}

func (r clientRouter) PutIPNS(ctx context.Context, name ipns.Name, record *ipns.Record) error {
	if !r.putEnabled {
		return routing.ErrNotSupported
	}

	return r.client.PutIPNS(ctx, name, record)
}

func (r clientRouter) provide(do func() (iter.ResultIter[*types.AnnouncementResponseRecord], error)) (time.Duration, error) {
	resultsIter, err := do()
	if err != nil {
		return 0, err
	}
	defer resultsIter.Close()

	records, err := iter.ReadAllResults(resultsIter)
	if err != nil {
		return 0, err
	}

	if len(records) != 1 {
		return 0, errors.New("invalid number of records returned")
	}

	return records[0].TTL, nil
}

func (r clientRouter) Close() error {
	return nil
}

var _ router = localRouter{}

type localRouter struct {
	datastore datastore.Batching
}

func providersKey(cid cid.Cid) datastore.Key {
	return datastore.KeyWithNamespaces([]string{"providers", cid.String()})
}

func peersKey(pid peer.ID) datastore.Key {
	return datastore.KeyWithNamespaces([]string{"peers", pid.String()})
}

func ipnsKey(name ipns.Name) datastore.Key {
	return datastore.NewKey("ipns-" + name.String())
}

func newLocalRouter(datadir string) (*localRouter, error) {
	ds, err := leveldb.NewDatastore(filepath.Join(datadir, "leveldb"), nil)
	if err != nil {
		return nil, err
	}

	return &localRouter{
		datastore: ds,
	}, nil
}

func (r localRouter) FindProviders(ctx context.Context, cid cid.Cid, limit int) (iter.ResultIter[types.Record], error) {
	raw, err := r.datastore.Get(ctx, providersKey(cid))
	if errors.Is(err, datastore.ErrNotFound) {
		return iter.ToResultIter(iter.FromSlice([]types.Record{})), nil
	} else if err != nil {
		return nil, err
	}

	var peerRecords []*types.PeerRecord
	err = json.Unmarshal(raw, &peerRecords)
	if err != nil {
		return nil, err
	}

	var records []types.Record
	for _, r := range peerRecords {
		records = append(records, r)
	}

	return iter.ToResultIter(iter.FromSlice(records)), nil
}

// Note: we don't verify the record since that is already done by the caller,
// i.e., Boxo's implementation of the server. This also facilitates our tests.
func (r localRouter) Provide(ctx context.Context, req *types.AnnouncementRecord) (time.Duration, error) {
	key := providersKey(req.Payload.CID)

	var records []types.PeerRecord

	raw, err := r.datastore.Get(ctx, key)
	if errors.Is(err, datastore.ErrNotFound) {
		// Nothing
	} else if err != nil {
		return 0, err
	} else {
		err = json.Unmarshal(raw, &records)
		if err != nil {
			return 0, err
		}
	}

	// NOTE: this is a very naive storage. We just transform the announcement
	// record into a peer record and append it to the list. This will be returned
	// when FindPeers is called.
	records = append(records, types.PeerRecord{
		Schema:    types.SchemaPeer,
		ID:        req.Payload.ID,
		Addrs:     req.Payload.Addrs,
		Protocols: req.Payload.Protocols,
	})

	raw, err = json.Marshal(records)
	if err != nil {
		return 0, err
	}

	return req.Payload.TTL, r.datastore.Put(ctx, key, raw)
}

func (r localRouter) FindPeers(ctx context.Context, pid peer.ID, limit int) (iter.ResultIter[*types.PeerRecord], error) {
	raw, err := r.datastore.Get(ctx, peersKey(pid))
	if errors.Is(err, datastore.ErrNotFound) {
		return iter.ToResultIter(iter.FromSlice([]*types.PeerRecord{})), nil
	} else if err != nil {
		return nil, err
	}

	var record *types.PeerRecord
	err = json.Unmarshal(raw, &record)
	if err != nil {
		return nil, err
	}

	return iter.ToResultIter(iter.FromSlice([]*types.PeerRecord{record})), nil
}

// Note: we don't verify the record since that is already done by the caller,
// i.e., Boxo's implementation of the server. This also facilitates our tests.
func (r localRouter) ProvidePeer(ctx context.Context, req *types.AnnouncementRecord) (time.Duration, error) {
	key := peersKey(*req.Payload.ID)

	// Make a [types.PeerRecord] based on the given [types.AnnouncementRecord].
	record := &types.PeerRecord{
		Schema:    types.SchemaPeer,
		ID:        req.Payload.ID,
		Addrs:     req.Payload.Addrs,
		Protocols: req.Payload.Protocols,
	}

	raw, err := r.datastore.Get(ctx, key)
	if errors.Is(err, datastore.ErrNotFound) {
		// Nothing
	} else if err != nil {
		return 0, err
	} else {
		// If we already had a record for the same peer, merge them together.
		var oldRecord *types.PeerRecord
		err = json.Unmarshal(raw, &oldRecord)
		if err != nil {
			return 0, err
		}

		record.Addrs = lo.Uniq(append(record.Addrs, oldRecord.Addrs...))
		record.Protocols = lo.Uniq(append(record.Protocols, oldRecord.Protocols...))
	}

	raw, err = json.Marshal(record)
	if err != nil {
		return 0, err
	}

	return req.Payload.TTL, r.datastore.Put(ctx, key, raw)
}

func (r localRouter) GetIPNS(ctx context.Context, name ipns.Name) (*ipns.Record, error) {
	raw, err := r.datastore.Get(ctx, ipnsKey(name))
	if errors.Is(err, datastore.ErrNotFound) {
		return nil, routing.ErrNotFound
	}
	if err != nil {
		return nil, err
	}

	return ipns.UnmarshalRecord(raw)
}

func (r localRouter) PutIPNS(ctx context.Context, name ipns.Name, record *ipns.Record) error {
	shouldStore, err := r.shouldStoreNewIPNS(ctx, name, record)
	if err != nil {
		return err
	}

	if !shouldStore {
		return nil
	}

	data, err := ipns.MarshalRecord(record)
	if err != nil {
		return err
	}

	return r.datastore.Put(ctx, ipnsKey(name), data)
}

func (r localRouter) shouldStoreNewIPNS(ctx context.Context, name ipns.Name, record *ipns.Record) (bool, error) {
	raw, err := r.datastore.Get(ctx, ipnsKey(name))
	if errors.Is(err, datastore.ErrNotFound) {
		return true, nil
	}

	if err != nil {
		return false, err
	}

	oldRecord, err := ipns.UnmarshalRecord(raw)
	if err != nil {
		return false, err
	}

	oldSequence, err := oldRecord.Sequence()
	if err != nil {
		return false, err
	}

	oldValidity, err := oldRecord.Validity()
	if err != nil {
		return false, err
	}

	sequence, err := record.Sequence()
	if err != nil {
		return false, err
	}

	validity, err := record.Validity()
	if err != nil {
		return false, err
	}

	// Only store new record if sequence is higher or the validity is higher.
	return sequence > oldSequence || validity.After(oldValidity), nil
}

func (r localRouter) Close() error {
	return r.datastore.Close()
}

var _ server.ContentRouter = sanitizeRouter{}

type sanitizeRouter struct {
	router
}

func (r sanitizeRouter) FindProviders(ctx context.Context, key cid.Cid, limit int) (iter.ResultIter[types.Record], error) {
	it, err := r.router.FindProviders(ctx, key, limit)
	if err != nil {
		return nil, err
	}

	return iter.Map(it, func(v iter.Result[types.Record]) iter.Result[types.Record] {
		if v.Err != nil || v.Val == nil {
			return v
		}

		switch v.Val.GetSchema() {
		case types.SchemaPeer:
			result, ok := v.Val.(*types.PeerRecord)
			if !ok {
				logger.Errorw("problem casting find providers result", "Schema", v.Val.GetSchema(), "Type", reflect.TypeOf(v).String())
				return v
			}

			result.Addrs = filterPrivateMultiaddr(result.Addrs)
			v.Val = result

		}

		return v
	}), nil
}

func (r sanitizeRouter) FindPeers(ctx context.Context, pid peer.ID, limit int) (iter.ResultIter[*types.PeerRecord], error) {
	it, err := r.router.FindPeers(ctx, pid, limit)
	if err != nil {
		return nil, err
	}

	return iter.Map(it, func(v iter.Result[*types.PeerRecord]) iter.Result[*types.PeerRecord] {
		if v.Err != nil || v.Val == nil {
			return v
		}

		v.Val.Addrs = filterPrivateMultiaddr(v.Val.Addrs)
		return v
	}), nil
}

func filterPrivateMultiaddr(a []types.Multiaddr) []types.Multiaddr {
	b := make([]types.Multiaddr, 0, len(a))

	for _, addr := range a {
		if manet.IsPrivateAddr(addr.Multiaddr) {
			continue
		}

		b = append(b, addr)
	}

	return b
}
