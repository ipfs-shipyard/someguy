package main

import (
	"context"
	"io"
	"sync"
	"sync/atomic"
	"time"

	lru "github.com/hashicorp/golang-lru/v2"
	"github.com/ipfs/boxo/routing/http/types"
	"github.com/libp2p/go-libp2p-kad-dht/amino"
	"github.com/libp2p/go-libp2p/core/event"
	"github.com/libp2p/go-libp2p/core/host"
	"github.com/libp2p/go-libp2p/core/network"
	"github.com/libp2p/go-libp2p/core/peer"
	"github.com/libp2p/go-libp2p/core/peerstore"
	"github.com/libp2p/go-libp2p/p2p/host/peerstore/pstoremem"
	"github.com/libp2p/go-libp2p/p2p/protocol/circuitv2/relay"
	ma "github.com/multiformats/go-multiaddr"
	manet "github.com/multiformats/go-multiaddr/net"
	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promauto"
)

const (
	Subsystem = "cached_addr_book"
	// The TTL to keep recently connected peers for. Same as [amino.DefaultProvideValidity] in go-libp2p-kad-dht
	RecentlyConnectedAddrTTL = amino.DefaultProvideValidity

	// Connected peers don't expire until they disconnect
	ConnectedAddrTTL = peerstore.ConnectedAddrTTL

	// How long to wait since last connection before probing a peer again
	PeerProbeThreshold = time.Hour

	// How often to run the probe peers loop
	ProbeInterval = time.Minute * 15

	// How many concurrent probes to run at once
	MaxConcurrentProbes = 20

	// How long to wait for a connect in a probe to complete.
	// The worst case is a peer behind a relay, so we use the relay connect timeout.
	ConnectTimeout = relay.ConnectTimeout

	// How many peers to cache in the peer state cache
	// 100_000 is also the default number of signed peer records cached by the memory address book.
	PeerCacheSize = 100_000
)

var (
	probeDurationHistogram = promauto.NewHistogram(prometheus.HistogramOpts{
		Name:      "probe_duration_seconds",
		Namespace: name,
		Subsystem: Subsystem,
		Help:      "Duration of peer probing operations in seconds",
		// Buckets probe durations from 1s to 5 minutes
		Buckets: []float64{1, 2, 5, 10, 30, 60, 120, 300},
	})

	probedPeersCounter = promauto.NewCounter(prometheus.CounterOpts{
		Name:      "probed_peers",
		Subsystem: Subsystem,
		Namespace: name,
		Help:      "Number of peers probed",
	})

	peerStateSize = promauto.NewGauge(prometheus.GaugeOpts{
		Name:      "peer_state_size",
		Subsystem: Subsystem,
		Namespace: name,
		Help:      "Number of peers object currently in the peer state",
	})
)

type peerState struct {
	lastConnTime       time.Time // last time we successfully connected to this peer
	lastFailedConnTime time.Time // last time we failed to find or connect to this peer
	connectFailures    uint      // number of times we've failed to connect to this peer
}

type cachedAddrBook struct {
	addrBook        peerstore.AddrBook                     // memory address book
	peerCache       *lru.TwoQueueCache[peer.ID, peerState] // LRU cache with additional metadata about peer
	isProbing       atomic.Bool
	allowPrivateIPs bool // for testing
}

type AddrBookOption func(*cachedAddrBook) error

func WithAllowPrivateIPs() AddrBookOption {
	return func(cab *cachedAddrBook) error {
		cab.allowPrivateIPs = true
		return nil
	}
}

func newCachedAddrBook(opts ...AddrBookOption) (*cachedAddrBook, error) {
	peerCache, err := lru.New2Q[peer.ID, peerState](PeerCacheSize)
	if err != nil {
		return nil, err
	}

	cab := &cachedAddrBook{
		peerCache: peerCache,
		addrBook:  pstoremem.NewAddrBook(),
	}

	for _, opt := range opts {
		err := opt(cab)
		if err != nil {
			return nil, err
		}
	}
	return cab, nil
}

func (cab *cachedAddrBook) background(ctx context.Context, host host.Host) {
	sub, err := host.EventBus().Subscribe([]interface{}{
		&event.EvtPeerIdentificationCompleted{},
		&event.EvtPeerConnectednessChanged{},
	})
	if err != nil {
		logger.Errorf("failed to subscribe to peer identification events: %v", err)
		return
	}
	defer sub.Close()

	probeTicker := time.NewTicker(ProbeInterval)
	defer probeTicker.Stop()

	for {
		select {
		case <-ctx.Done():
			cabCloser, ok := cab.addrBook.(io.Closer)
			if ok {
				errClose := cabCloser.Close()
				if errClose != nil {
					logger.Warnf("failed to close addr book: %v", errClose)
				}
			}
			return
		case ev := <-sub.Out():
			switch ev := ev.(type) {
			case event.EvtPeerIdentificationCompleted:
				pState, exists := cab.peerCache.Get(ev.Peer)
				if !exists {
					pState = peerState{}
				}
				pState.lastConnTime = time.Now()
				pState.lastFailedConnTime = time.Time{} // reset failed connection time
				pState.connectFailures = 0              // reset connect failures on successful connection
				cab.peerCache.Add(ev.Peer, pState)
				peerStateSize.Set(float64(cab.peerCache.Len())) // update metric

				ttl := getTTL(host.Network().Connectedness(ev.Peer))
				if ev.SignedPeerRecord != nil {
					logger.Debug("Caching signed peer record")
					cab, ok := peerstore.GetCertifiedAddrBook(cab.addrBook)
					if ok {
						_, err := cab.ConsumePeerRecord(ev.SignedPeerRecord, ttl)
						if err != nil {
							logger.Warnf("failed to consume signed peer record: %v", err)
						}
					}
				} else {
					logger.Debug("No signed peer record, caching listen addresses")
					// We don't have a signed peer record, so we use the listen addresses
					cab.addrBook.AddAddrs(ev.Peer, ev.ListenAddrs, ttl)
				}
			case event.EvtPeerConnectednessChanged:
				// If the peer is not connected or limited, we update the TTL
				if !hasValidConnectedness(ev.Connectedness) {
					cab.addrBook.UpdateAddrs(ev.Peer, ConnectedAddrTTL, RecentlyConnectedAddrTTL)
				}
			}
		case <-probeTicker.C:
			if cab.isProbing.Load() {
				logger.Debug("Skipping peer probe, still running")
				continue
			}
			logger.Debug("Starting to probe peers")
			go cab.probePeers(ctx, host)
		}
	}
}

// Loops over all peers with addresses and probes them if they haven't been probed recently
func (cab *cachedAddrBook) probePeers(ctx context.Context, host host.Host) {
	cab.isProbing.Store(true)
	defer cab.isProbing.Store(false)

	start := time.Now()
	defer func() {
		duration := time.Since(start).Seconds()
		probeDurationHistogram.Observe(duration)
		logger.Debugf("Finished probing peers in %s", duration)
	}()

	var wg sync.WaitGroup
	// semaphore channel to limit the number of concurrent probes
	semaphore := make(chan struct{}, MaxConcurrentProbes)

	for i, p := range cab.addrBook.PeersWithAddrs() {
		if hasValidConnectedness(host.Network().Connectedness(p)) {
			continue // don't probe connected peers
		}

		if !cab.ShouldProbePeer(p) {
			continue
		}

		addrs := cab.addrBook.Addrs(p)

		if !cab.allowPrivateIPs {
			addrs = ma.FilterAddrs(addrs, manet.IsPublicAddr)
		}

		if len(addrs) == 0 {
			continue // no addresses to probe
		}

		wg.Add(1)
		go func() {
			semaphore <- struct{}{}
			defer func() {
				<-semaphore // Release semaphore
				wg.Done()
			}()
			probedPeersCounter.Inc()
			ctx, cancel := context.WithTimeout(ctx, ConnectTimeout)
			defer cancel()
			logger.Debugf("Probe %d: PeerID: %s, Addrs: %v", i+1, p, addrs)
			// if connect succeeds and identify runs, the background loop will take care of updating the peer state and cache
			err := host.Connect(ctx, peer.AddrInfo{
				ID:    p,
				Addrs: addrs,
			})
			if err != nil {
				logger.Debugf("failed to connect to peer %s: %v", p, err)
				cab.RecordFailedConnection(p)
			}
		}()
	}
	wg.Wait()
}

// Returns the cached addresses for a peer, incrementing the return count
func (cab *cachedAddrBook) GetCachedAddrs(p peer.ID) []types.Multiaddr {
	cachedAddrs := cab.addrBook.Addrs(p)

	if len(cachedAddrs) == 0 {
		return nil
	}

	var result []types.Multiaddr // convert to local Multiaddr type 🙃
	for _, addr := range cachedAddrs {
		result = append(result, types.Multiaddr{Multiaddr: addr})
	}
	return result
}

// Update the peer cache with information about a failed connection
// This should be called when a connection attempt to a peer fails
func (cab *cachedAddrBook) RecordFailedConnection(p peer.ID) {
	pState, exists := cab.peerCache.Get(p)
	if !exists {
		pState = peerState{}
	}
	pState.lastFailedConnTime = time.Now()
	pState.connectFailures++
	cab.peerCache.Add(p, pState)
}

// Returns true if we should probe a peer (either by dialing known addresses or by dispatching a FindPeer)
// based on the last failed connection time and connection failures
func (cab *cachedAddrBook) ShouldProbePeer(p peer.ID) bool {
	pState, exists := cab.peerCache.Get(p)
	if !exists {
		return true // default to probing if the peer is not in the cache
	}

	var backoffDuration time.Duration
	if pState.connectFailures > 0 {
		// Calculate backoff only if we have failures
		// this is effectively 2^(connectFailures - 1) * PeerProbeThreshold
		// A single failure results in a 1 hour backoff
		backoffDuration = PeerProbeThreshold * time.Duration(1<<(pState.connectFailures-1))
	} else {
		backoffDuration = PeerProbeThreshold
	}

	// Only dispatch if we've waited long enough based on the backoff
	return time.Since(pState.lastFailedConnTime) > backoffDuration
}

func hasValidConnectedness(connectedness network.Connectedness) bool {
	return connectedness == network.Connected || connectedness == network.Limited
}

func getTTL(connectedness network.Connectedness) time.Duration {
	if hasValidConnectedness(connectedness) {
		return ConnectedAddrTTL
	}
	return RecentlyConnectedAddrTTL
}
