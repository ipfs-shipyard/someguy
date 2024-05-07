package main

import (
	"context"
	"crypto/rand"
	"errors"
	"testing"
	"time"

	"github.com/ipfs/boxo/ipns"
	"github.com/ipfs/boxo/path"
	"github.com/ipfs/boxo/routing/http/types"
	"github.com/ipfs/boxo/routing/http/types/iter"
	"github.com/ipfs/go-cid"
	"github.com/libp2p/go-libp2p/core/crypto"
	"github.com/libp2p/go-libp2p/core/peer"
	"github.com/libp2p/go-libp2p/core/routing"
	"github.com/multiformats/go-multiaddr"
	"github.com/multiformats/go-multihash"
	"github.com/stretchr/testify/mock"
	"github.com/stretchr/testify/require"
)

type mockRouter struct{ mock.Mock }

var _ router = &mockRouter{}

func (m *mockRouter) FindProviders(ctx context.Context, key cid.Cid, limit int) (iter.ResultIter[types.Record], error) {
	args := m.Called(ctx, key, limit)
	if arg0 := args.Get(0); arg0 == nil {
		return nil, args.Error(1)
	}
	return args.Get(0).(iter.ResultIter[types.Record]), args.Error(1)
}

func (m *mockRouter) Provide(ctx context.Context, req *types.AnnouncementRecord) (time.Duration, error) {
	args := m.Called(ctx, req)
	return args.Get(0).(time.Duration), args.Error(1)
}

func (m *mockRouter) FindPeers(ctx context.Context, pid peer.ID, limit int) (iter.ResultIter[*types.PeerRecord], error) {
	args := m.Called(ctx, pid, limit)
	if arg0 := args.Get(0); arg0 == nil {
		return nil, args.Error(1)
	}
	return args.Get(0).(iter.ResultIter[*types.PeerRecord]), args.Error(1)
}

func (m *mockRouter) ProvidePeer(ctx context.Context, req *types.AnnouncementRecord) (time.Duration, error) {
	args := m.Called(ctx, req)
	return args.Get(0).(time.Duration), args.Error(1)
}

func (m *mockRouter) GetIPNS(ctx context.Context, name ipns.Name) (*ipns.Record, error) {
	args := m.Called(ctx, name)
	if arg0 := args.Get(0); arg0 == nil {
		return nil, args.Error(1)
	}
	return args.Get(0).(*ipns.Record), args.Error(1)
}

func (m *mockRouter) PutIPNS(ctx context.Context, name ipns.Name, record *ipns.Record) error {
	args := m.Called(ctx, name, record)
	return args.Error(0)
}

func (m *mockRouter) Close() error {
	return nil
}

func makeName(t *testing.T) (crypto.PrivKey, ipns.Name) {
	sk, _, err := crypto.GenerateEd25519Key(rand.Reader)
	require.NoError(t, err)

	pid, err := peer.IDFromPrivateKey(sk)
	require.NoError(t, err)

	return sk, ipns.NameFromPeer(pid)
}

func makeIPNSRecord(t *testing.T, sk crypto.PrivKey, opts ...ipns.Option) (*ipns.Record, []byte) {
	cid, err := cid.Decode("bafkreifjjcie6lypi6ny7amxnfftagclbuxndqonfipmb64f2km2devei4")
	require.NoError(t, err)

	path := path.FromCid(cid)
	eol := time.Now().Add(time.Hour * 48)
	ttl := time.Second * 20

	record, err := ipns.NewRecord(sk, path, 1, eol, ttl, opts...)
	require.NoError(t, err)

	rawRecord, err := ipns.MarshalRecord(record)
	require.NoError(t, err)

	return record, rawRecord
}

func TestGetIPNS(t *testing.T) {
	t.Parallel()

	sk, name := makeName(t)
	rec, _ := makeIPNSRecord(t, sk)

	t.Run("OK (Multiple Composable, One Fails, One OK)", func(t *testing.T) {
		ctx := context.Background()

		mr1 := &mockRouter{}
		mr1.On("GetIPNS", mock.Anything, name).Return(rec, nil)

		mr2 := &mockRouter{}
		mr2.On("GetIPNS", mock.Anything, name).Return(nil, routing.ErrNotFound)

		r := parallelRouter{
			routers: []router{
				composableRouter{
					ipns: mr1,
				},
				composableRouter{
					ipns: mr2,
				},
			},
		}

		getRec, err := r.GetIPNS(ctx, name)
		require.NoError(t, err)
		require.EqualValues(t, rec, getRec)
	})

	t.Run("OK (Multiple Parallel)", func(t *testing.T) {
		ctx := context.Background()

		mr1 := &mockRouter{}
		mr1.On("GetIPNS", mock.Anything, name).Return(nil, routing.ErrNotFound)

		mr2 := &mockRouter{}
		mr2.On("GetIPNS", mock.Anything, name).Return(rec, nil)

		r := parallelRouter{
			routers: []router{
				composableRouter{
					ipns: parallelRouter{
						routers: []router{mr1, mr2},
					},
				},
			},
		}

		getRec, err := r.GetIPNS(ctx, name)
		require.NoError(t, err)
		require.EqualValues(t, rec, getRec)
	})

	t.Run("No Routers", func(t *testing.T) {
		ctx := context.Background()

		r := parallelRouter{
			routers: []router{
				composableRouter{
					ipns: parallelRouter{},
				},
			},
		}

		_, err := r.GetIPNS(ctx, name)
		require.ErrorIs(t, err, routing.ErrNotFound)
	})
}

func TestPutIPNS(t *testing.T) {
	t.Parallel()

	sk, name := makeName(t)
	rec, _ := makeIPNSRecord(t, sk)

	t.Run("OK (Multiple Composable)", func(t *testing.T) {
		ctx := context.Background()

		mr1 := &mockRouter{}
		mr1.On("PutIPNS", mock.Anything, name, rec).Return(nil)

		mr2 := &mockRouter{}
		mr2.On("PutIPNS", mock.Anything, name, rec).Return(nil)

		r := parallelRouter{
			routers: []router{
				composableRouter{
					ipns: mr1,
				},
				composableRouter{
					ipns: mr2,
				},
			},
		}

		err := r.PutIPNS(ctx, name, rec)
		require.NoError(t, err)

		mr1.AssertExpectations(t)
		mr2.AssertExpectations(t)
	})

	t.Run("OK (Multiple Parallel)", func(t *testing.T) {
		ctx := context.Background()

		mr1 := &mockRouter{}
		mr1.On("PutIPNS", mock.Anything, name, rec).Return(nil)

		mr2 := &mockRouter{}
		mr2.On("PutIPNS", mock.Anything, name, rec).Return(nil)

		r := parallelRouter{
			routers: []router{
				composableRouter{
					ipns: parallelRouter{
						routers: []router{mr1, mr2},
					},
				},
			},
		}

		err := r.PutIPNS(ctx, name, rec)
		require.NoError(t, err)

		mr1.AssertExpectations(t)
		mr2.AssertExpectations(t)
	})

	t.Run("Failure of a Single Router (Multiple Composable)", func(t *testing.T) {
		ctx := context.Background()

		mr1 := &mockRouter{}
		mr1.On("PutIPNS", mock.Anything, name, rec).Return(errors.New("failed"))

		mr2 := &mockRouter{}
		mr2.On("PutIPNS", mock.Anything, name, rec).Return(nil)

		r := parallelRouter{
			routers: []router{
				composableRouter{
					ipns: mr1,
				},
				composableRouter{
					ipns: mr2,
				},
			},
		}

		err := r.PutIPNS(ctx, name, rec)
		require.ErrorContains(t, err, "failed")

		mr1.AssertExpectations(t)
		mr2.AssertExpectations(t)
	})

	t.Run("Failure of a Single Router (Multiple Parallel)", func(t *testing.T) {
		ctx := context.Background()

		mr1 := &mockRouter{}
		mr1.On("PutIPNS", mock.Anything, name, mock.Anything).Return(errors.New("failed"))

		mr2 := &mockRouter{}
		mr2.On("PutIPNS", mock.Anything, name, mock.Anything).Return(nil)

		r := parallelRouter{
			routers: []router{
				composableRouter{
					ipns: parallelRouter{
						routers: []router{mr1, mr2},
					},
				},
			},
		}

		err := r.PutIPNS(ctx, name, rec)
		require.ErrorContains(t, err, "failed")

		mr1.AssertExpectations(t)
		mr2.AssertExpectations(t)
	})
}

func makeCID() cid.Cid {
	buf := make([]byte, 63)
	_, err := rand.Read(buf)
	if err != nil {
		panic(err)
	}
	mh, err := multihash.Encode(buf, multihash.SHA2_256)
	if err != nil {
		panic(err)
	}
	c := cid.NewCidV1(cid.Raw, mh)
	return c
}

func mustMultiaddr(t *testing.T, s string) types.Multiaddr {
	ma, err := multiaddr.NewMultiaddr(s)
	require.NoError(t, err)
	return types.Multiaddr{Multiaddr: ma}
}

func TestFindProviders(t *testing.T) {
	t.Parallel()

	t.Run("Basic", func(t *testing.T) {
		ctx := context.Background()
		c := makeCID()
		peers := []peer.ID{"peer1", "peer2", "peer3"}

		var d router
		d = parallelRouter{}
		it, err := d.FindProviders(ctx, c, 10)

		require.NoError(t, err)
		require.False(t, it.Next())

		mr1 := &mockRouter{}
		mr1Iter := newMockIter[types.Record](ctx)
		mr1.On("FindProviders", mock.Anything, c, 10).Return(mr1Iter, nil)

		mr2 := &mockRouter{}
		mr2Iter := newMockIter[types.Record](ctx)
		mr2.On("FindProviders", mock.Anything, c, 10).Return(mr2Iter, nil)

		d = sanitizeRouter{parallelRouter{
			routers: []router{
				&composableRouter{
					providers: mr1,
				},
				mr2,
			},
		}}

		privateAddr := mustMultiaddr(t, "/ip4/192.168.1.123/tcp/4001")
		loopbackAddr := mustMultiaddr(t, "/ip4/127.0.0.1/tcp/4001")
		publicAddr := mustMultiaddr(t, "/ip4/137.21.14.12/tcp/4001")

		go func() {
			mr1Iter.ch <- iter.Result[types.Record]{Val: &types.PeerRecord{
				Schema: "peer",
				ID:     &peers[0],
				Addrs:  []types.Multiaddr{privateAddr, loopbackAddr, publicAddr},
			}}
			mr2Iter.ch <- iter.Result[types.Record]{Val: &types.PeerRecord{Schema: "peer", ID: &peers[0]}}
			mr1Iter.ch <- iter.Result[types.Record]{Val: &types.PeerRecord{Schema: "peer", ID: &peers[1]}}
			mr1Iter.ch <- iter.Result[types.Record]{Val: &types.PeerRecord{Schema: "peer", ID: &peers[2]}}
			close(mr1Iter.ch)

			mr2Iter.ch <- iter.Result[types.Record]{Val: &types.PeerRecord{Schema: "peer", ID: &peers[1]}}
			close(mr2Iter.ch)
		}()

		it, err = d.FindProviders(ctx, c, 10)
		require.NoError(t, err)

		results, err := iter.ReadAllResults(it)
		require.NoError(t, err)
		require.Len(t, results, 5)

		require.Len(t, results[0].(*types.PeerRecord).Addrs, 1)
		require.Equal(t, publicAddr.String(), results[0].(*types.PeerRecord).Addrs[0].String())
	})

	t.Run("Failed to Create All Iterators", func(t *testing.T) {
		ctx := context.Background()
		c := makeCID()

		mr1 := &mockRouter{}
		mr1.On("FindProviders", mock.Anything, c, 10).Return(nil, errors.New("error a"))

		mr2 := &mockRouter{}
		mr2.On("FindProviders", mock.Anything, c, 10).Return(nil, errors.New("error b"))

		d := parallelRouter{
			routers: []router{
				mr1, mr2,
			},
		}

		_, err := d.FindProviders(ctx, c, 10)
		require.ErrorContains(t, err, "error a")
		require.ErrorContains(t, err, "error b")
	})

	t.Run("Failed to Create One Iterator", func(t *testing.T) {
		ctx := context.Background()
		pid := peer.ID("hello")
		c := makeCID()

		mr1 := &mockRouter{}
		mr1.On("FindProviders", mock.Anything, c, 10).Return(iter.ToResultIter(iter.FromSlice([]types.Record{&types.PeerRecord{Schema: "peer", ID: &pid}})), nil)

		mr2 := &mockRouter{}
		mr2.On("FindProviders", mock.Anything, c, 10).Return(nil, errors.New("error b"))

		d := parallelRouter{
			routers: []router{
				mr1, mr2,
			},
		}

		it, err := d.FindProviders(ctx, c, 10)
		require.NoError(t, err)

		results, err := iter.ReadAllResults(it)
		require.NoError(t, err)
		require.Len(t, results, 1)
	})
}

func TestFindPeers(t *testing.T) {
	t.Parallel()

	t.Run("Basic", func(t *testing.T) {
		ctx := context.Background()
		pid := peer.ID("hello")

		d := parallelRouter{}
		it, err := d.FindPeers(ctx, pid, 10)

		require.NoError(t, err)
		require.False(t, it.Next())

		mr1 := &mockRouter{}
		mr1Iter := newMockIter[*types.PeerRecord](ctx)
		mr1.On("FindPeers", mock.Anything, pid, 10).Return(mr1Iter, nil)

		mr2 := &mockRouter{}
		mr2Iter := newMockIter[*types.PeerRecord](ctx)
		mr2.On("FindPeers", mock.Anything, pid, 10).Return(mr2Iter, nil)

		d = parallelRouter{
			routers: []router{
				&composableRouter{
					peers: mr1,
				},
				mr2,
			},
		}

		go func() {
			mr1Iter.ch <- iter.Result[*types.PeerRecord]{Val: &types.PeerRecord{Schema: "peer", ID: &pid}}
			mr2Iter.ch <- iter.Result[*types.PeerRecord]{Val: &types.PeerRecord{Schema: "peer", ID: &pid}}
			mr1Iter.ch <- iter.Result[*types.PeerRecord]{Val: &types.PeerRecord{Schema: "peer", ID: &pid}}
			mr1Iter.ch <- iter.Result[*types.PeerRecord]{Val: &types.PeerRecord{Schema: "peer", ID: &pid}}
			close(mr1Iter.ch)

			mr2Iter.ch <- iter.Result[*types.PeerRecord]{Val: &types.PeerRecord{Schema: "peer", ID: &pid}}
			close(mr2Iter.ch)
		}()

		it, err = d.FindPeers(ctx, pid, 10)
		require.NoError(t, err)

		results, err := iter.ReadAllResults(it)
		require.NoError(t, err)
		require.Len(t, results, 5)
	})

	t.Run("Failed to Create All Iterators", func(t *testing.T) {
		ctx := context.Background()
		pid := peer.ID("hello")

		mr1 := &mockRouter{}
		mr1.On("FindPeers", mock.Anything, pid, 10).Return(nil, errors.New("error a"))

		mr2 := &mockRouter{}
		mr2.On("FindPeers", mock.Anything, pid, 10).Return(nil, errors.New("error b"))

		d := parallelRouter{
			routers: []router{
				mr1, mr2,
			},
		}

		_, err := d.FindPeers(ctx, pid, 10)
		require.ErrorContains(t, err, "error a")
		require.ErrorContains(t, err, "error b")
	})

	t.Run("Failed to Create One Iterator", func(t *testing.T) {
		ctx := context.Background()
		pid := peer.ID("hello")

		mr1 := &mockRouter{}
		mr1.On("FindPeers", mock.Anything, pid, 10).Return(iter.ToResultIter(iter.FromSlice([]*types.PeerRecord{{Schema: "peer", ID: &pid}})), nil)

		mr2 := &mockRouter{}
		mr2.On("FindPeers", mock.Anything, pid, 10).Return(nil, errors.New("error b"))

		d := parallelRouter{
			routers: []router{
				mr1, mr2,
			},
		}

		it, err := d.FindPeers(ctx, pid, 10)
		require.NoError(t, err)

		results, err := iter.ReadAllResults(it)
		require.NoError(t, err)
		require.Len(t, results, 1)
	})
}

type mockIter[T any] struct {
	ctx     context.Context
	ch      chan iter.Result[T]
	waitVal chan time.Time
	val     iter.Result[T]
	done    bool
}

var _ iter.ResultIter[int] = &mockIter[int]{}

func newMockIter[T any](ctx context.Context) *mockIter[T] {
	it := &mockIter[T]{
		ctx: ctx,
		ch:  make(chan iter.Result[T]),
	}

	return it
}

func newMockIters[T any](ctx context.Context, count int) []*mockIter[T] {
	var arr []*mockIter[T]

	for count > 0 {
		arr = append(arr, newMockIter[T](ctx))
		count--
	}

	return arr
}

func (m *mockIter[T]) Next() bool {
	if m.done {
		return false
	}

	select {
	case v, ok := <-m.ch:
		if !ok {
			m.done = true
		} else {
			m.val = v
		}
	case <-m.ctx.Done():
		m.done = true
	}

	return !m.done
}

func (m *mockIter[T]) Val() iter.Result[T] {
	if m.waitVal != nil {
		<-m.waitVal
	}

	return m.val
}

func (m *mockIter[T]) Close() error {
	m.done = true
	return nil
}

func mockItersAsInterface[T any](originalSlice []*mockIter[T]) []iter.ResultIter[T] {
	var newSlice []iter.ResultIter[T]

	for _, v := range originalSlice {
		newSlice = append(newSlice, v)
	}

	return newSlice
}

func TestManyIter(t *testing.T) {
	t.Parallel()

	t.Run("Sequence", func(t *testing.T) {
		t.Parallel()

		ctx, cancel := context.WithCancel(context.Background())
		defer cancel()

		its := newMockIters[int](ctx, 2)
		manyIter := newManyIter(ctx, mockItersAsInterface(its))

		go func() {
			its[0].ch <- iter.Result[int]{Val: 0}
			time.Sleep(time.Millisecond * 50)

			its[1].ch <- iter.Result[int]{Val: 1}
			time.Sleep(time.Millisecond * 50)

			its[0].ch <- iter.Result[int]{Val: 0}
			time.Sleep(time.Millisecond * 50)

			its[0].ch <- iter.Result[int]{Val: 0}
			close(its[0].ch)
			time.Sleep(time.Millisecond * 50)

			its[1].ch <- iter.Result[int]{Val: 1}
			time.Sleep(time.Millisecond * 50)

			close(its[1].ch)
		}()

		results, err := iter.ReadAllResults(manyIter)
		require.NoError(t, err)
		require.Equal(t, []int{0, 1, 0, 0, 1}, results)
		require.False(t, manyIter.Next())
		require.NoError(t, manyIter.Close())
	})

	t.Run("Closed Iterator", func(t *testing.T) {
		t.Parallel()

		ctx, cancel := context.WithCancel(context.Background())
		defer cancel()

		its := newMockIters[int](ctx, 5)
		manyIter := newManyIter(ctx, mockItersAsInterface(its))

		go func() {
			close(its[0].ch)
			close(its[1].ch)
			close(its[2].ch)
			close(its[3].ch)

			its[4].ch <- iter.Result[int]{Val: 4}
			time.Sleep(time.Millisecond * 50)

			its[4].ch <- iter.Result[int]{Val: 4}
			time.Sleep(time.Millisecond * 50)

			close(its[4].ch)
		}()

		results, err := iter.ReadAllResults(manyIter)
		require.NoError(t, err)
		require.Equal(t, []int{4, 4}, results)
		require.False(t, manyIter.Next())
		require.NoError(t, manyIter.Close())
	})

	t.Run("Context Canceled", func(t *testing.T) {
		t.Parallel()

		ctx, cancel := context.WithCancel(context.Background())
		defer cancel()

		its := newMockIters[int](ctx, 5)
		manyIter := newManyIter(ctx, mockItersAsInterface(its))

		go func() {
			its[3].ch <- iter.Result[int]{Val: 3}
			time.Sleep(time.Millisecond * 50)

			its[2].ch <- iter.Result[int]{Val: 2}
			time.Sleep(time.Millisecond * 50)

			cancel()
		}()

		results, err := iter.ReadAllResults(manyIter)
		require.NoError(t, err)
		require.Equal(t, []int{3, 2}, results)
		require.False(t, manyIter.Next())
		require.NoError(t, manyIter.Close())
	})

	t.Run("Context Canceled After .Next Returns", func(t *testing.T) {
		t.Parallel()

		ctx, cancel := context.WithCancel(context.Background())
		defer cancel()

		its := newMockIters[int](ctx, 5)
		manyIter := newManyIter(ctx, mockItersAsInterface(its))

		go func() {
			its[1].ch <- iter.Result[int]{Val: 1}
			time.Sleep(time.Millisecond * 50)

			its[4].ch <- iter.Result[int]{Val: 4}
			time.Sleep(time.Millisecond * 50)

			its[3].waitVal = make(chan time.Time)
			its[3].ch <- iter.Result[int]{Val: 3}
			time.Sleep(time.Millisecond * 50)

			cancel()
			time.Sleep(time.Millisecond * 50)

			its[3].waitVal <- time.Now()
		}()

		results, err := iter.ReadAllResults(manyIter)
		require.NoError(t, err)
		require.Equal(t, []int{1, 4}, results)
		require.False(t, manyIter.Next())
		require.NoError(t, manyIter.Close())
	})
}

func equalRecords(t *testing.T, a, b *ipns.Record) {
	aValue, err := a.Value()
	require.NoError(t, err)

	aSequence, err := a.Sequence()
	require.NoError(t, err)

	aTTL, err := a.TTL()
	require.NoError(t, err)

	aValidity, err := a.Validity()
	require.NoError(t, err)

	bValue, err := b.Value()
	require.NoError(t, err)

	bSequence, err := b.Sequence()
	require.NoError(t, err)

	bTTL, err := b.TTL()
	require.NoError(t, err)

	bValidity, err := b.Validity()
	require.NoError(t, err)

	require.Equal(t, aValue, bValue)
	require.Equal(t, aSequence, bSequence)
	require.Equal(t, aTTL, bTTL)
	require.Equal(t, aValidity, bValidity)
}

func TestLocalRouter(t *testing.T) {
	t.Parallel()

	t.Run("Providers", func(t *testing.T) {
		t.Parallel()

		tempDir := t.TempDir()
		r, err := newLocalRouter(tempDir)
		require.NoError(t, err)
		defer r.Close()

		_, name := makeName(t)
		pid := name.Peer()
		cid := makeCID()

		t.Run("Store and Retrieve Record for CID", func(t *testing.T) {
			resultsIter, err := r.FindProviders(context.Background(), cid, 10)
			require.NoError(t, err)

			results, err := iter.ReadAllResults(resultsIter)
			require.NoError(t, err)
			require.Len(t, results, 0)

			_, err = r.Provide(context.Background(), &types.AnnouncementRecord{
				Payload: types.AnnouncementPayload{
					CID:       cid,
					ID:        &pid,
					Protocols: []string{"a"},
				},
			})
			require.NoError(t, err)

			resultsIter, err = r.FindProviders(context.Background(), cid, 10)
			require.NoError(t, err)

			results, err = iter.ReadAllResults(resultsIter)
			require.NoError(t, err)
			require.Len(t, results, 1)
		})

		t.Run("Provide New Record", func(t *testing.T) {
			_, err = r.Provide(context.Background(), &types.AnnouncementRecord{
				Payload: types.AnnouncementPayload{
					ID:        &pid,
					CID:       cid,
					Protocols: []string{"b", "a", "a"},
				},
			})
			require.NoError(t, err)

			resultsIter, err := r.FindProviders(context.Background(), cid, 10)
			require.NoError(t, err)

			results, err := iter.ReadAllResults(resultsIter)
			require.NoError(t, err)
			require.Len(t, results, 2)
		})
	})

	t.Run("Peers", func(t *testing.T) {
		t.Parallel()

		tempDir := t.TempDir()
		r, err := newLocalRouter(tempDir)
		require.NoError(t, err)
		defer r.Close()

		_, name := makeName(t)
		pid := name.Peer()

		t.Run("Store and Retrieve Peer", func(t *testing.T) {
			resultsIter, err := r.FindPeers(context.Background(), pid, 1)
			require.NoError(t, err)

			results, err := iter.ReadAllResults(resultsIter)
			require.NoError(t, err)
			require.Len(t, results, 0)

			_, err = r.ProvidePeer(context.Background(), &types.AnnouncementRecord{
				Payload: types.AnnouncementPayload{
					ID:        &pid,
					Protocols: []string{"a"},
				},
			})
			require.NoError(t, err)

			resultsIter, err = r.FindPeers(context.Background(), pid, 1)
			require.NoError(t, err)

			results, err = iter.ReadAllResults(resultsIter)
			require.NoError(t, err)
			require.Len(t, results, 1)
			require.Equal(t, []string{"a"}, results[0].Protocols)
		})

		t.Run("Provide Updated Information", func(t *testing.T) {
			_, err = r.ProvidePeer(context.Background(), &types.AnnouncementRecord{
				Payload: types.AnnouncementPayload{
					ID:        &pid,
					Protocols: []string{"b", "a", "a"},
				},
			})
			require.NoError(t, err)

			resultsIter, err := r.FindPeers(context.Background(), pid, 1)
			require.NoError(t, err)

			results, err := iter.ReadAllResults(resultsIter)
			require.NoError(t, err)
			require.Len(t, results, 1)
			require.Equal(t, []string{"b", "a"}, results[0].Protocols)
		})
	})

	t.Run("IPNS", func(t *testing.T) {
		t.Parallel()

		tempDir := t.TempDir()
		r, err := newLocalRouter(tempDir)
		require.NoError(t, err)
		defer r.Close()

		sk, name := makeName(t)
		rec, _ := makeIPNSRecord(t, sk)

		time.Sleep(time.Millisecond * 100)
		newerRecord, _ := makeIPNSRecord(t, sk)

		t.Run("Store and Retrieve IPNS Record", func(t *testing.T) {
			_, err = r.GetIPNS(context.Background(), name)
			require.ErrorIs(t, err, routing.ErrNotFound)

			err = r.PutIPNS(context.Background(), name, rec)
			require.NoError(t, err)

			storedRecord, err := r.GetIPNS(context.Background(), name)
			require.NoError(t, err)
			equalRecords(t, rec, storedRecord)
		})

		t.Run("Should Replace With Newer Record", func(t *testing.T) {
			err = r.PutIPNS(context.Background(), name, newerRecord)
			require.NoError(t, err)

			storedRecord, err := r.GetIPNS(context.Background(), name)
			require.NoError(t, err)
			equalRecords(t, newerRecord, storedRecord)
		})

		t.Run("Should Not Replace With Older Record", func(t *testing.T) {
			err = r.PutIPNS(context.Background(), name, rec)
			require.NoError(t, err)

			storedRecord, err := r.GetIPNS(context.Background(), name)
			require.NoError(t, err)
			equalRecords(t, newerRecord, storedRecord)
		})
	})

}
