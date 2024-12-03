package main

import (
	"context"
	"sync/atomic"

	"github.com/ipfs/boxo/routing/http/types"
	"github.com/ipfs/boxo/routing/http/types/iter"
	"github.com/ipfs/go-cid"
	"github.com/libp2p/go-libp2p/core/peer"
	ma "github.com/multiformats/go-multiaddr"
	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promauto"
)

var (
	_ router = cachedRouter{}

	// peerAddrLookups allows us reason if/how effective peer addr cache is
	peerAddrLookups = promauto.NewCounterVec(prometheus.CounterOpts{
		Name:      "peer_addr_lookups",
		Subsystem: "cached_router",
		Namespace: name,
		Help:      "Number of peer addr info lookups per origin and cache state",
	},
		[]string{addrCacheStateLabel, addrQueryOriginLabel},
	)
)

const (
	// cache=unused|hit|miss, indicates how effective cache is
	addrCacheStateLabel  = "cache"
	addrCacheStateUnused = "unused"
	addrCacheStateHit    = "hit"
	addrCacheStateMiss   = "miss"

	// source=providers|peers indicates if query originated from provider or peer endpoint
	addrQueryOriginLabel     = "origin"
	addrQueryOriginProviders = "providers"
	addrQueryOriginPeers     = "peers"
	addrQueryOriginUnknown   = "unknown"
)

// cachedRouter wraps a router with the cachedAddrBook to retrieve cached addresses for peers without multiaddrs in FindProviders
type cachedRouter struct {
	router
	cachedAddrBook *cachedAddrBook
}

func NewCachedRouter(router router, cab *cachedAddrBook) cachedRouter {
	return cachedRouter{router, cab}
}

func (r cachedRouter) FindProviders(ctx context.Context, key cid.Cid, limit int) (iter.ResultIter[types.Record], error) {
	it, err := r.router.FindProviders(ctx, key, limit)
	if err != nil {
		return nil, err
	}

	return NewCacheFallbackIter(it, r, ctx), nil // create a new iterator that will use cache if available and fallback to `FindPeer` if no addresses are cached
}

// TODO: Open question: should we implement FindPeers to look up cache? If a FindPeer fails to return any peers, the peer is likely long offline.
// func (r cachedRouter) FindPeers(ctx context.Context, pid peer.ID, limit int) (iter.ResultIter[*types.PeerRecord], error) {
// 	return r.router.FindPeers(ctx, pid, limit)
// }

// withAddrsFromCache returns the best list of addrs for specified [peer.ID].
// It will consult cache only if the addrs slice passed to it is empty.
func (r cachedRouter) withAddrsFromCache(queryOrigin string, pid *peer.ID, addrs []types.Multiaddr) []types.Multiaddr {
	// skip cache if we already have addrs
	if len(addrs) > 0 {
		peerAddrLookups.WithLabelValues(addrCacheStateUnused, queryOrigin).Inc()
		return addrs
	}

	cachedAddrs := r.cachedAddrBook.GetCachedAddrs(pid)
	if len(cachedAddrs) > 0 {
		logger.Debugw("found cached addresses", "peer", pid, "cachedAddrs", cachedAddrs)
		peerAddrLookups.WithLabelValues(addrCacheStateHit, queryOrigin).Inc()
		return cachedAddrs
	} else {
		// Cache miss. Queue peer for lookup.
		peerAddrLookups.WithLabelValues(addrCacheStateMiss, queryOrigin).Inc()
		return nil
	}
}

var _ iter.ResultIter[types.Record] = &cacheFallbackIter{}

// cacheFallbackIter is a custom iterator that will resolve peers with no addresses from cache and if no cached addresses, will look them up via FindPeers.
type cacheFallbackIter struct {
	sourceIter      iter.ResultIter[types.Record]
	current         iter.Result[types.Record]
	findPeersResult chan *types.PeerRecord
	router          cachedRouter
	ctx             context.Context
	ongoingLookups  atomic.Int32
}

func NewCacheFallbackIter(sourceIter iter.ResultIter[types.Record], router cachedRouter, ctx context.Context) *cacheFallbackIter {
	return &cacheFallbackIter{
		sourceIter: sourceIter,
		router:     router,
		ctx:        ctx,

		findPeersResult: make(chan *types.PeerRecord, 1),
		ongoingLookups:  atomic.Int32{},
	}
}

func (it *cacheFallbackIter) Next() bool {
	select {
	case <-it.ctx.Done():
		return false
	case foundPeer := <-it.findPeersResult:
		// read from channel if available
		it.current = iter.Result[types.Record]{Val: foundPeer}
		return true
	default:
		// load up current val from source iterator and avoid blocking on channel
		if it.sourceIter.Next() {
			val := it.sourceIter.Val()
			switch val.Val.GetSchema() {
			case types.SchemaBitswap:
				result, ok := val.Val.(*types.BitswapRecord)
				if !ok {
					it.current = val
					return true // pass these through
				}
				result.Addrs = it.router.withAddrsFromCache(addrQueryOriginProviders, result.ID, result.Addrs)
				if result.Addrs != nil {
					it.current = iter.Result[types.Record]{Val: result}
					return true
				} else {
					// no cached addrs, queue for lookup and try to get the next value from the source iterator
					go it.dispatchFindPeer(*result.ID)
					if it.sourceIter.Next() {
						it.current = it.sourceIter.Val()
						return true
					} else {
						return it.ongoingLookups.Load() > 0 // if the source iterator is exhausted, check if there are any peers left to look up
					}
				}

			case types.SchemaPeer:
				result, ok := val.Val.(*types.PeerRecord)
				if !ok {
					it.current = val
					return true // pass these through
				}
				result.Addrs = it.router.withAddrsFromCache(addrQueryOriginProviders, result.ID, result.Addrs)
				if result.Addrs != nil {
					it.current = iter.Result[types.Record]{Val: result}
					return true
				} else {
					// no cached addrs, queue for lookup and try to get the next value from the source iterator
					go it.dispatchFindPeer(*result.ID)
					if it.sourceIter.Next() {
						it.current = it.sourceIter.Val()
						return true
					} else {
						return it.ongoingLookups.Load() > 0 // if the source iterator is exhausted, check if there are any peers left to look up
					}
				}
			}
		}
		// source iterator is exhausted, check if there are any peers left to look up
		if it.ongoingLookups.Load() > 0 {
			// if there are any ongoing lookups, return true to keep iterating
			return true
		}
		// if there are no ongoing lookups and the source iterator is exhausted, we're done
		return false
	}
}

func (it *cacheFallbackIter) dispatchFindPeer(pid peer.ID) {
	it.ongoingLookups.Add(1)
	defer it.ongoingLookups.Add(-1)
	// FindPeers is weird in that it accepts a limit. But we only want one result, ideally from the libp2p router.
	peersIt, err := it.router.FindPeers(it.ctx, pid, 1)

	if err != nil {
		logger.Errorw("error looking up peer", "peer", pid, "error", err)
		return
	}
	peers, err := iter.ReadAllResults(peersIt)
	if err != nil {
		logger.Errorw("error reading find peers results", "peer", pid, "error", err)
		return
	}
	if len(peers) > 0 {
		it.findPeersResult <- peers[0]
	} else {
		logger.Errorw("no peer was found in cachedFallbackIter", "peer", pid)
	}
}

func (it *cacheFallbackIter) Val() iter.Result[types.Record] {
	return it.current
}

func (it *cacheFallbackIter) Close() error {
	it.ctx.Cancel()
	close(it.findPeersResult)
	return it.sourceIter.Close()
}

func ToMultiaddrs(addrs []ma.Multiaddr) []types.Multiaddr {
	var result []types.Multiaddr
	for _, addr := range addrs {
		result = append(result, types.Multiaddr{Multiaddr: addr})
	}
	return result
}
